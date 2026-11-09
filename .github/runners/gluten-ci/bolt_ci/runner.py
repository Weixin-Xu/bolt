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

"""Reconcile durable worker claims and validate bounded report evidence."""

import hashlib
import json
import logging
import os
import re
import stat
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from xml.etree import ElementTree

from .docker import DockerError, INSTANCE
from .github import ApiError
from .policy import HASH
from .state import COUNTS

MAX_JSON = 4 * 1024 * 1024
MAX_XML = 64 * 1024 * 1024
MAX_TPC = 128 * 1024 * 1024
MAX_ARTIFACT = 16 * 1024**3
MAX_TESTS = 10_000_000
ZERO = dict.fromkeys(COUNTS, 0)
NATIVE = "native.tar.gz"
MANIFEST = "native-manifest.json"
LOADER = "org.apache.gluten.jni.BoltNativeLoaderSuite"
LABEL = "io.bolt.gluten-ci."


def relative_path(path):
    if not isinstance(path, str) or not path or len(path) > 4096 or "\x00" in path:
        raise ValueError("Invalid worker evidence path")
    rel = PurePosixPath(path)
    if (
        rel.is_absolute()
        or not rel.parts
        or rel.as_posix() != path
        or ".." in rel.parts
    ):
        raise ValueError("Invalid worker evidence path")
    return rel


@contextmanager
def regular(root, relative, maximum):
    """Open each directory component by descriptor, without following symlinks."""
    rel = relative_path(relative)
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("Invalid worker output directory")
    dirs = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY
    directory = os.open("/", dirs)
    try:
        for part in (*root.parts[1:], *rel.parts[:-1]):
            child = os.open(part, dirs, dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(
            rel.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_size > maximum
            ):
                raise ValueError("Invalid worker output file")
            yield stream
    finally:
        os.close(directory)


def contents(root, relative, maximum):
    with regular(root, relative, maximum) as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("Worker output exceeded size limit")
    return data


def json_report(root, name):
    data = json.loads(contents(root, name, MAX_JSON))
    if not isinstance(data, dict):
        raise ValueError("Invalid worker JSON")
    return data


def xml_counts(root, name):
    xml = contents(root, name, MAX_XML).decode("utf-8")
    if "<!DOCTYPE" in xml or "<!ENTITY" in xml:
        raise ValueError("Invalid worker XML declarations")
    try:
        tree = ElementTree.fromstring(xml)
    except ElementTree.ParseError:
        raise ValueError("Invalid worker XML") from None
    if tree.tag not in {"testsuite", "testsuites"}:
        raise ValueError("Unexpected worker XML")
    counts = ZERO.copy()
    for case in tree.iter("testcase"):
        counts["total"] += 1
        if case.find("failure") is not None or case.find("error") is not None:
            counts["failed"] += 1
        elif case.find("skipped") is not None:
            counts["skipped"] += 1
        else:
            counts["passed"] += 1
    return counts


def tpc_counts(root, name):
    pattern = re.compile(rb"Summary: ([0-9]{1,8}) out of ([0-9]{1,8}) queries passed\.")
    match = None
    with regular(root, name, MAX_TPC) as stream:
        while line := stream.readline(8192):
            parsed = pattern.fullmatch(line.strip())
            if parsed:
                if match:
                    raise ValueError("Duplicate benchmark counts")
                match = parsed
    if not match:
        raise ValueError("Missing benchmark counts")
    passed, total = map(int, match.groups())
    if passed > total:
        raise ValueError("Invalid benchmark counts")
    return dict(total=total, passed=passed, failed=total - passed, skipped=0)


def jvm_groups(root, evidence):
    names = (
        contents(root, "jvm/discovered-suites.txt", 1024 * 1024).decode().splitlines()
    )
    if (
        not names
        or len(set(names)) != len(names)
        or any(not re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_.$]*", name) for name in names)
    ):
        raise ValueError("Invalid JVM test selection")
    delta = {name for name in names if name.startswith("org.apache.spark.sql.delta.")}
    ordinary = set(names) - delta - {LOADER}
    if not delta or not ordinary or LOADER not in names:
        raise ValueError("Missing required JVM test groups")
    groups = {"backend": ordinary, **{suite: {suite} for suite in delta | {LOADER}}}
    paths = {}
    for name in evidence:
        match = re.fullmatch(
            r"jvm/([^/]+)/backends-bolt/target/[^/]*reports/TEST-([^/]+)\.xml", name
        )
        if match and match[1] in groups and match[2] in groups[match[1]]:
            paths.setdefault(match[1], []).append(name)
    return [paths.get(group, []) for group in groups]


def test_evidence(entry, root, evidence):
    stage = entry["stage"]
    parse = xml_counts
    if entry["kind"] == "native":
        groups = [["TEST-native.xml"]]
    elif stage == "tpc":
        parse = tpc_counts
        groups = [[f"tpc-{name}/run.log"] for name in entry["selector"].split(",")]
    elif stage == "jvm":
        groups = jvm_groups(root, evidence)
    else:
        spark = entry["spark"].replace(".", "")
        prefix = re.escape(f"{stage}/gluten-ut/spark{spark}/target/")
        groups = [
            [
                name
                for name in evidence
                if re.fullmatch(prefix + r"[^/]*reports/TEST-[^/]+\.xml", name)
            ]
        ]
    total = ZERO.copy()
    for group in groups:
        executed = 0
        for path in group:
            counts = parse(root, path)
            executed += counts["passed"] + counts["failed"]
            for key in COUNTS:
                total[key] += counts[key]
        if not executed:
            raise ValueError("A required test group executed zero tests")
    return total


def collect_result(config, task, root, exit_code):
    result = json_report(root, "result.json")
    expected = config.identity(task["head_sha"])
    if json_report(root, "sources.json") != expected:
        raise ValueError("Worker report identity mismatch")
    counts = result.get("tests")
    evidence = result.get("evidence")
    if (
        type(result.get("schema")) is not int
        or result["schema"] != 1
        or result.get("task") != task["task"]
        or type(result.get("exit_code")) is not int
        or result["exit_code"] != exit_code
        or result.get("status") != ("failure" if exit_code else "success")
        or not isinstance(counts, dict)
        or set(counts) != set(COUNTS)
        or any(
            type(value) is not int or not 0 <= value <= MAX_TESTS
            for value in counts.values()
        )
        or counts["total"] != counts["passed"] + counts["failed"] + counts["skipped"]
        or not isinstance(evidence, list)
        or len(evidence) > 20000
    ):
        raise ValueError("Invalid worker result")
    paths = {relative_path(path).as_posix() for path in evidence}
    if len(paths) != len(evidence):
        raise ValueError("Duplicate worker evidence")
    entry = config.tasks[task["task"]]
    if entry["kind"] == "package":
        if counts != ZERO:
            raise ValueError("Unexpected packaging test counts")
        if not exit_code:
            verification = contents(root, "package-verification.txt", 4096).decode()
            expected_record = rf"[0-9a-f]{{64}}  (gluten-bolt-bundle-[A-Za-z0-9._+-]+\.jar)\nbolt_revision={config.bolt_sha}\n"
            match = re.fullmatch(expected_record, verification)
            if not match or match[1] not in paths:
                raise ValueError("Missing package verification")
            with regular(root, match[1], MAX_ARTIFACT):
                pass
    elif not exit_code or counts["failed"]:
        if test_evidence(entry, root, paths) != counts:
            raise ValueError("Worker test evidence does not match its report")
    if exit_code:
        return (
            "failure",
            "test" if counts["failed"] else "build",
            counts if counts["failed"] else ZERO,
        )
    if counts["failed"]:
        raise ValueError("Failed tests in successful worker result")
    return "success", None, counts


def seal_native(config, task, source, destination):
    expected = config.identity(task["head_sha"])
    manifest = json_report(source, MANIFEST)
    archive_hash = manifest.get("archive_sha256")
    if (
        set(manifest) != {"schema", "archive_sha256", *expected}
        or type(manifest["schema"]) is not int
        or manifest["schema"] != 1
        or any(manifest[key] != value for key, value in expected.items())
        or not isinstance(archive_hash, str)
        or not HASH.fullmatch(archive_hash)
    ):
        raise ValueError("Native artifact identity mismatch")
    if destination.exists():
        with regular(destination, NATIVE, MAX_ARTIFACT) as archive:
            if (
                json_report(destination, MANIFEST) == manifest
                and hashlib.file_digest(archive, "sha256").hexdigest() == archive_hash
            ):
                return
        raise ValueError("Previously sealed native artifact differs")
    with tempfile.TemporaryDirectory(dir=destination.parent, prefix=".native-") as temp:
        staging = Path(temp) / "artifact"
        staging.mkdir(mode=0o755)
        staging.chmod(0o755)
        checksum = hashlib.sha256()
        size = 0
        with (
            regular(source, NATIVE, MAX_ARTIFACT) as archive,
            (staging / NATIVE).open("xb") as output,
        ):
            while block := archive.read(1024 * 1024):
                size += len(block)
                if size > MAX_ARTIFACT:
                    raise ValueError("Native artifact exceeded size limit")
                checksum.update(block)
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
        if size == 0 or checksum.hexdigest() != archive_hash:
            raise ValueError("Native artifact checksum mismatch")
        with (staging / MANIFEST).open("x") as output:
            json.dump(manifest, output)
            output.flush()
            os.fsync(output.fileno())
        for path in staging.iterdir():
            path.chmod(0o444)
        staging.rename(destination)
        directory = os.open(destination.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


class Runner:
    def __init__(
        self, config, state, api, docker, workspaces, *, clock=time.time, cache=None
    ):
        self.config = config
        self.state = state
        self.api = api
        self.docker = docker
        self.workspaces = workspaces
        self.clock = clock
        self.cache = cache

    def _labels(self, task):
        return {
            INSTANCE: self.state.instance,
            LABEL + "request": str(task["request_id"]),
            LABEL + "ordinal": str(task["ordinal"]),
            LABEL + "source": task["head_sha"],
            LABEL + "bundle": task["bundle"],
        }

    def _owns(self, task, container):
        return (
            container.name == task["container"]
            and (task["docker_id"] is None or task["docker_id"] == container.id)
            and all(
                container.labels.get(key) == value
                for key, value in self._labels(task).items()
            )
        )

    def _verified(self, task):
        if not self.state.current_task(task):
            return False
        pr = self.api.pull(task["pr"])
        current = pr.open and pr.identity == tuple(
            task[key] for key in ("head_sha", "head_repo", "base_sha", "base_repo")
        )
        if not current:
            self.state.invalidate(task)
        return current and self.state.current_task(task)

    def _fail(self, task, now, category):
        self.state.finish(task, "failure", category, None, now)

    def _collect(self, task, container, now):
        name = task["container"]
        job = self.workspaces.job(task)
        self.docker.logs(name, job / "worker.log")
        if container.oom:
            self._fail(task, now, "infrastructure")
            return
        try:
            status, category, counts = collect_result(
                self.config, task, job / "reports", container.exit_code
            )
            if task["kind"] == "native" and status == "success":
                seal_native(
                    self.config,
                    task,
                    job / "native-out",
                    self.workspaces.artifact(task),
                )
        except (OSError, ValueError, RecursionError):
            self._fail(task, now, "evidence")
            return
        self.state.finish(task, status, category, counts, now)

    def _reconcile(self, task, now, safe):
        name = task["container"]
        container = self.docker.inspect(name)
        if not container or not self._owns(task, container):
            self._fail(task, now, task["stop_reason"] or "interrupted")
            return
        if task["docker_id"] is None and not self.state.remember_container(
            task, container.id
        ):
            self._fail(task, now, "interrupted")
            return
        task = task | {"docker_id": container.id}
        out_of_scope = (
            self.config.only_pr is not None and self.config.only_pr != task["pr"]
        )
        invalid = (
            task["request_state"] == "superseded"
            or not safe
            or out_of_scope
            or task["bundle"] != self.config.fingerprint
        )
        expired = now - task["claimed"] >= self.config.task_timeout_seconds
        stop = task["stop_reason"]
        if not stop and invalid:
            stop = "interrupted"
        if not stop and expired and container.status != "exited":
            stop = "timeout"
        if stop:
            self.state.mark_stopping(task, stop)
            if container.status not in {"created", "exited", "dead"}:
                self.docker.stop(name)
                remaining = self.docker.inspect(name)
                if remaining and remaining.status not in {"created", "exited", "dead"}:
                    return
            self._fail(task, now, stop)
            return
        if container.status == "created":
            if not self._verified(task):
                self._fail(task, now, "interrupted")
                return
            try:
                self.docker.start(name)
            except DockerError:
                # A lost start response is ambiguous, so inspect before failing.
                after = self.docker.inspect(name)
                if (
                    not after
                    or not self._owns(task, after)
                    or after.status == "created"
                ):
                    self._fail(task, now, "infrastructure")
                    return
                container = after
            else:
                container = self.docker.inspect(name)
            if not container or not self._owns(task, container):
                self._fail(task, now, "interrupted")
                return
        if container.status == "running":
            self.state.mark_started(task, now)
        elif container.status == "exited":
            if self._verified(task):
                self.state.mark_started(task, now)
                self._collect(task, container, now)
            else:
                self._fail(task, now, "interrupted")
        else:
            if container.status not in {"created", "dead"}:
                self.docker.stop(name)
            self._fail(task, now, "interrupted")

    def _launch(self, task, now):
        if task["bundle"] != self.config.fingerprint:
            self._fail(task, now, "interrupted")
            return
        try:
            if not self._verified(task):
                self._fail(task, now, "interrupted")
                return
            self.workspaces.stage(task)
        except (ApiError, OSError, ValueError, RuntimeError):
            self._fail(task, int(self.clock()), "infrastructure")
            return
        if not self.docker.verify_network():
            self._fail(task, int(self.clock()), "interrupted")
            return
        try:
            self.docker.create(task, self.workspaces, self._labels(task))
        except DockerError:
            # The durable deterministic name survives a lost create response.
            pass
        self._reconcile(task, int(self.clock()), self.docker.verify_network())

    def _cleanup(self, names):
        for task in self.state.cleanup_ready(names):
            name = task["container"]
            container = self.docker.inspect(name) if name in names else None
            if container and self._owns(task, container):
                if container.status not in {"created", "exited", "dead"}:
                    self.docker.stop(name)
                    continue
                job = self.workspaces.job(task)
                if not (job / "worker.log").exists():
                    self.docker.logs(name, job / "worker.log")
                self.docker.remove(name)
            if self.cache and task["kind"] != "native":
                self.cache(self.workspaces.job(task) / "cache/maven")
            self.workspaces.clean(task)
            self.state.cleaned(task)

    def tick(self):
        with self.state.lock("scheduler") as locked:
            if not locked:
                return
            config = self.config
            safe = self.docker.verify_network()
            for task in self.state.active():
                try:
                    self._reconcile(task, int(self.clock()), safe)
                except (ApiError, DockerError) as error:
                    logging.warning("Worker reconciliation deferred (%s)", error)
            self._cleanup(self.docker.names(self.state.instance))
            now = int(self.clock())
            self.state.expire(now, config.queue_timeout_seconds, config.only_pr)
            if not safe:
                logging.warning(
                    "Worker scheduling paused pending verified network isolation"
                )
                return
            for _ in range(config.max_parallel):
                if not self.docker.verify_network():
                    break
                now = int(self.clock())
                task = self.state.claim(
                    now,
                    config.queue_timeout_seconds,
                    config.max_parallel,
                    config.package_parallel,
                    config.only_pr,
                )
                if not task:
                    break
                self._launch(task, now)
