# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deliver a single field-only status comment per PR from the durable outbox."""

import json
import logging
import time
from collections import Counter

from .config import TASK
from .github import ApiError, positive
from .policy import valid_sha
from .state import COUNTS

MARKER = "<!-- gluten-bolt-external-ci:v1 -->"
TITLES = {
    "queued": "Queued",
    "running": "Running",
    "success": "Passed",
    "failure": "Failed",
    "queue_timeout": "Queue wait expired",
    "not_applicable": "Not applicable",
}
CATEGORIES = {
    "build": "build",
    "test": "tests",
    "timeout": "runtime timeout",
    "interrupted": "interrupted",
    "infrastructure": "infrastructure",
    "evidence": "invalid evidence",
}


def summary(event, tasks):
    sha, run, attempt = event["head_sha"], event["id"], event["attempt"]
    if not valid_sha(sha) or not positive(run) or not positive(attempt):
        raise ValueError("Invalid publication identity")
    status = event["publication"]
    lines = [
        MARKER,
        f"**Gluten Bolt CI — {TITLES[status]}**",
        f"Commit: `{sha}` | Run: `{run}` | Attempt: {attempt}",
    ]
    if status == "queued":
        lines.append(
            "Waiting for an executor. Scope: native UT, 14 package jobs, 27 Spark test groups."
        )
    elif status == "running":
        lines.append(
            "Execution started. Scope: native UT, 14 package jobs, 27 Spark test groups."
        )
    elif status == "queue_timeout":
        lines.append("The queue wait limit expired. Tests were not run.")
    elif status == "not_applicable":
        lines.append("No Bolt-related changes in the current PR diff.")
    else:
        by_kind = {kind: Counter() for kind in ("native", "package", "spark")}
        counts = Counter()
        failed = []
        for task in tasks:
            kind, task_state = task["kind"], task["state"]
            by_kind[kind][task_state] += 1
            if task["counts"]:
                result = json.loads(task["counts"])
                for key in COUNTS:
                    value = result.get(key)
                    if type(value) is int and 0 <= value <= 10_000_000:
                        counts[key] += value
            if task_state == "failure" and TASK.fullmatch(task["task"]):
                failed.append(
                    f"`{task['task']}` ({CATEGORIES.get(task['category'], 'infrastructure')})"
                )
        for kind, label in (
            ("native", "Native"),
            ("package", "Package jobs"),
            ("spark", "Spark groups"),
        ):
            results = by_kind[kind]
            lines.append(
                f"{label}: {results['success']} passed, {results['failure']} failed, {results['not_run']} not run"
            )
        lines.append(
            f"Tests: {counts['passed']} passed, {counts['failed']} failed, {counts['skipped']} skipped."
        )
        if failed:
            lines.append("Failed groups: " + ", ".join(failed))
        lines.append(f"Report: `bolt-{run}`")
    lines.append("To rerun, comment: `Run Gluten Bolt CI`")
    return "\n\n".join(lines)


class Commenter:
    def __init__(self, state, reader, writer, *, only_pr=None, clock=time.time):
        self.state = state
        self.reader = reader
        self.writer = writer
        self.only_pr = only_pr
        self.clock = clock
        self.viewer_id = None

    def _own(self, number):
        matches = []
        for comment in self.writer.comments(number):
            if not isinstance(comment, dict):
                continue
            user = comment.get("user")
            body = comment.get("body")
            if (
                isinstance(user, dict)
                and user.get("id") == self.viewer_id
                and positive(comment.get("id"))
                and isinstance(body, str)
                and MARKER in body.splitlines()
            ):
                matches.append(comment["id"])
        known = self.state.comment_id(number)
        return known if known in matches else min(matches, default=None)

    @staticmethod
    def _same(event, pr):
        return pr.open and pr.identity == tuple(
            event[key] for key in ("head_sha", "head_repo", "base_sha", "base_repo")
        )

    def _neutralize(self, comment_id, pr):
        if comment_id is not None and pr.open and valid_sha(pr.head_sha):
            body = f"{MARKER}\n\n**Gluten Bolt CI**\n\nAwaiting results for current commit `{pr.head_sha}`."
            self.writer.update_comment(comment_id, body)

    def _write(self, event):
        number = event["pr"]
        if not self.state.current(event):
            return
        if self.viewer_id is None:
            self.viewer_id = self.writer.viewer()
        # Discover the marker even if the database lacks a comment ID. A previous
        # POST may have succeeded before an HTTP timeout or controller restart.
        owned = self._own(number)
        current = self.reader.pull(number)
        if not self.state.current(event):
            return
        if not self._same(event, current):
            self._neutralize(owned, current)
            self.state.delivered(event)
            return
        if owned is None and event["publication"] == "not_applicable":
            self.state.delivered(event)
            return
        body = summary(event, self.state.task_rows(event["id"]))
        self.state.publishing(number)
        if owned is None:
            owned = self.writer.post_comment(number, body)
        else:
            self.writer.update_comment(owned, body)
        self.state.record_comment(number, owned)
        latest = self.state.latest(number)
        after = self.reader.pull(number)
        if not self._same(event, after) or (latest and latest["id"] != event["id"]):
            self._neutralize(owned, after)
        self.state.delivered(event)

    def tick(self):
        for event in self.state.pending(int(self.clock()), self.only_pr):
            with self.state.lock(f"comment-{event['pr']}") as locked:
                if not locked:
                    continue
                try:
                    self._write(event)
                except ApiError as error:
                    wait = min(3600, 15 * 2 ** min(event["failures"], 8))
                    self.state.retry(
                        event, int(self.clock()) + max(wait, error.retry_after)
                    )
                    logging.warning("Comment delivery deferred (%s)", error)
