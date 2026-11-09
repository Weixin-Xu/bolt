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

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from zipfile import ZipFile

from bolt_ci.commenter import Commenter, MARKER
from bolt_ci.config import Config
from bolt_ci.docker import Docker, Container, Workspaces, checkout
from bolt_ci.github import ApiError
from bolt_ci.runner import Runner, collect_result
from bolt_ci.state import State
from test_policy_listener import A, B, C, Api, pull


class WorkerDocker:
    def __init__(self):
        self.containers = {}
        self.launches = []
        self.lose_create_response = False
        self.fail_start = False
        self.isolated = True
        self.stopped = []

    def verify_network(self):
        return self.isolated

    def names(self, instance):
        return set(self.containers)

    def inspect(self, name):
        return self.containers.get(name)

    def create(self, task, workspaces, labels):
        name = task["container"]
        self.launches.append(task)
        self.containers[name] = Container(
            name, labels, "created", 0, False, hashlib.sha256(name.encode()).hexdigest()
        )
        if self.lose_create_response:
            self.lose_create_response = False
            from bolt_ci.docker import DockerError

            raise DockerError()

    def start(self, name):
        if self.fail_start:
            from bolt_ci.docker import DockerError

            raise DockerError()
        old = self.containers[name]
        self.containers[name] = replace(old, status="running")

    def stop(self, name):
        self.stopped.append(name)
        old = self.containers[name]
        self.containers[name] = replace(old, status="exited", exit_code=143)

    def logs(self, name, destination):
        destination.write_text("Private 192.0.2.8 /work/secret\n")

    def remove(self, name):
        self.containers.pop(name, None)


class Staging(Workspaces):
    def stage(self, task):
        for directory in ("source", "bolt", "reports", "cache", "native-out"):
            self.job(task).joinpath(directory).mkdir(parents=True)


class Comments:
    def __init__(self):
        self.posts = 0
        self.messages = {}
        self.denied = False
        self.lose_post = False
        self.after_write = lambda: None

    def viewer(self):
        return 701

    def comments(self, number):
        return list(self.messages.values())

    def post_comment(self, number, body):
        if self.denied:
            raise ApiError(403, 10)
        self.posts += 1
        self.messages[90] = {"id": 90, "user": {"id": 701}, "body": body}
        self.after_write()
        if self.lose_post:
            self.lose_post = False
            raise ApiError()
        return 90

    def update_comment(self, comment_id, body):
        if self.denied:
            raise ApiError(429, 10)
        self.messages[comment_id]["body"] = body
        self.after_write()


class ServicesTests(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        root = Path(work.name).resolve()
        helper = root / "helper"
        ci = helper / "dev/bolt-ci"
        ci.mkdir(parents=True)
        supplied = os.getenv("BOLT_TEST_HELPERS")
        if supplied:
            matrix = json.loads(
                (Path(supplied) / "dev/bolt-ci/matrix.json").read_text()
            )
        else:
            matrix = [
                {
                    "id": "native",
                    "kind": "native",
                    "jdk": 17,
                    "spark": None,
                    "stage": "native",
                    "profiles": [],
                },
                *(
                    {
                        "id": f"package:fixture-{index}",
                        "kind": "package",
                        "jdk": 17,
                        "spark": "3.5",
                        "stage": "package",
                        "profiles": [],
                    }
                    for index in range(14)
                ),
                *(
                    {
                        "id": f"spark:3.5:fixture-{index}",
                        "kind": "spark",
                        "jdk": 17,
                        "spark": "3.5",
                        "stage": f"fixture-{index}",
                        "profiles": [],
                    }
                    for index in range(27)
                ),
            ]
        (ci / "matrix.json").write_text(json.dumps(matrix))
        for asset in (
            "dev/bolt-ci/run.py",
            "dev/bolt-ci/run-task.sh",
            "dev/ci-bolt-build.sh",
            "dev/ci-bolt-spark-env.sh",
        ):
            (helper / asset).touch()
        self.config = Config.from_mapping(
            dict(
                repository="apache/gluten",
                host_data_dir=str(root),
                state_dir=str(root / "state"),
                helper_dir=str(helper),
                bolt_checkout=str(root / "engine"),
                driver_dir=str(root / "driver"),
                jdk_dirs={
                    str(jdk): str(root / f"jdk{jdk}") for jdk in (8, 11, 17, 21, 25)
                },
                image="localhost:5000/bolt-ci@sha256:" + "a" * 64,
                bolt_sha=A,
                driver_sha=B,
                helper_sha=C,
                network="gluten-worker",
                network_preflight_file=str(root / "network.json"),
                read_token_file=str(root / "reader"),
                bot_token_file=str(root / "writer"),
                max_parallel=4,
                package_parallel=2,
            )
        )
        self.db = State(
            self.config.state_dir,
            self.config.repository,
            self.config.matrix,
            self.config.fingerprint,
        )
        self.api = Api()
        self.api.current[1] = pull()
        self.db.observe(self.api.current[1], 1000, related=True)
        self.docker = WorkerDocker()
        self.staging = Staging(self.config)
        self.now = 1000
        self.comment_api = Comments()

    def runner(self):
        return Runner(
            self.config,
            self.db,
            self.api,
            self.docker,
            self.staging,
            clock=lambda: self.now,
        )

    def commenter(self):
        return Commenter(self.db, self.api, self.comment_api, clock=lambda: self.now)

    def complete(self, task, *, fail=False, invalid=False):
        job = self.staging.job(task)
        reports = job / "reports"
        identity = self.config.identity(task["head_sha"])
        (reports / "sources.json").write_text(json.dumps(identity))
        entry = self.config.tasks[task["task"]]
        evidence = []
        if task["kind"] == "native":
            evidence = ["TEST-native.xml"]
        elif task["kind"] == "spark":
            stage, spark = entry["stage"], entry["spark"].replace(".", "")
            if stage == "tpc":
                evidence = [f"tpc-{kind}/run.log" for kind in ("h", "ds")]
            elif stage == "jvm":
                suites = [
                    "org.apache.spark.BoltSuite",
                    "org.apache.gluten.jni.BoltNativeLoaderSuite",
                    "org.apache.spark.sql.delta.DeltaSuite",
                ]
                evidence = [
                    f"jvm/{group}/backends-bolt/target/surefire-reports/TEST-{suite}.xml"
                    for group, suite in zip(["backend", *suites[1:]], suites)
                ]
                (reports / "jvm").mkdir()
                (reports / "jvm/discovered-suites.txt").write_text("\n".join(suites))
                evidence.append("jvm/discovered-suites.txt")
            else:
                evidence = [
                    f"{stage}/gluten-ut/spark{spark}/target/surefire-reports/TEST-Unit.xml"
                ]
        else:
            jar = reports / "gluten-bolt-bundle-1.jar"
            with ZipFile(jar, "w") as bundle:
                bundle.writestr("gluten-build-info.properties", f"bolt_revision={A}\n")
            evidence = ["package-verification.txt", jar.name]
        for relative in evidence:
            path = reports / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if relative.endswith(".xml"):
                path.write_text("<testsuite><testcase/></testsuite>")
            elif relative.endswith("run.log"):
                path.write_text("  Summary: 1 out of 1 queries passed.\n")
            elif relative == "package-verification.txt":
                digest = hashlib.sha256(jar.read_bytes()).hexdigest()
                path.write_text(f"{digest}  {jar.name}\nbolt_revision={A}\n")
            elif not path.exists():
                path.write_text("verified")
        count = sum(path.endswith((".xml", "run.log")) for path in evidence)
        result = dict(
            schema=1,
            task=task["task"],
            status="failure" if fail else "success",
            exit_code=int(fail),
            tests=dict(total=count, passed=count, failed=0, skipped=0),
            evidence=evidence,
        )
        if invalid:
            result["evidence"].append("../../../secrets")
        (reports / "result.json").write_text(json.dumps(result))
        if task["kind"] == "native":
            artifact = job / "native-out/native.tar.gz"
            artifact.write_bytes(b"native payload")
            manifest = dict(
                schema=1,
                archive_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                **identity,
            )
            (artifact.parent / "native-manifest.json").write_text(json.dumps(manifest))
        name = task["container"]
        old = self.docker.containers[name]
        self.docker.containers[name] = replace(
            old, status="exited", exit_code=int(fail)
        )

    def test_restart_reconciles_ambiguous_create_and_full_graph_without_fail_fast(self):
        self.docker.lose_create_response = True
        self.commenter().tick()
        self.assertIn("Queued", self.comment_api.messages[90]["body"])
        self.runner().tick()
        self.commenter().tick()
        self.assertIn("Running", self.comment_api.messages[90]["body"])
        self.assertEqual([task["task"] for task in self.docker.launches], ["native"])
        self.complete(self.db.active()[0])
        failed = False
        for _ in range(50):
            self.runner().tick()
            active = self.db.active()
            if not active:
                break
            self.assertLessEqual(len(active), 4)
            self.assertLessEqual(sum(task["kind"] == "package" for task in active), 2)
            for task in active:
                should_fail = task["kind"] == "package" and not failed
                self.complete(task, fail=should_fail)
                failed |= should_fail
        self.assertEqual(len(self.docker.launches), 42)
        self.assertEqual(len({task["container"] for task in self.docker.launches}), 42)
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.commenter().tick()
        body = self.comment_api.messages[90]["body"]
        self.assertIn("13 passed, 1 failed", body)
        self.assertIn("27 passed, 0 failed", body)
        self.assertNotIn("192.0.2.8", body)
        self.assertNotIn("/work/secret", body)
        self.assertEqual(self.comment_api.posts, 1)

    def test_package_requires_nonempty_jar_matching_verification_digest(self):
        self.runner().tick()
        self.complete(self.db.active()[0])
        self.runner().tick()
        task = next(task for task in self.db.active() if task["kind"] == "package")
        self.complete(task)
        reports = self.staging.job(task) / "reports"
        jar = reports / "gluten-bolt-bundle-1.jar"
        verification = reports / "package-verification.txt"
        payload, record = jar.read_bytes(), verification.read_text()

        self.assertEqual(collect_result(self.config, task, reports, 0)[0], "success")

        for label, changed in (
            ("replacement", payload[:-1] + bytes([payload[-1] ^ 1])),
            ("truncation", payload[: len(payload) // 2]),
            ("empty with matching digest", b""),
        ):
            with self.subTest(label=label):
                jar.write_bytes(changed)
                if not changed:
                    empty_digest = hashlib.sha256(changed).hexdigest()
                    verification.write_text(empty_digest + record[64:])
                with self.assertRaises(ValueError):
                    collect_result(self.config, task, reports, 0)

    def test_invalid_native_report_blocks_entire_downstream(self):
        self.runner().tick()
        self.complete(self.db.active()[0], invalid=True)
        self.runner().tick()
        self.assertEqual(len(self.docker.launches), 1)
        tasks = self.db.task_rows(self.db.latest(1)["id"])
        self.assertEqual(
            [row["state"] for row in tasks], ["failure"] + ["not_run"] * 41
        )

    def test_failed_start_cannot_be_queue_timeout(self):
        self.docker.fail_start = True
        self.runner().tick()
        self.now += 1800
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_missing_claimed_container_after_restart_is_interrupted(self):
        self.db.claim(self.now, 1800, 4, 2)
        self.now += 1800
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.assertEqual(self.docker.launches, [])

    def test_network_receipt_loss_stops_running_and_refuses_new_work(self):
        self.runner().tick()
        self.docker.isolated = False
        self.runner().tick()
        self.assertEqual(len(self.docker.stopped), 1)
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_ambiguous_post_and_api_denial_recover_without_duplicate_or_rebuild(self):
        self.comment_api.denied = True
        self.commenter().tick()
        self.assertEqual(self.db.status()["pending_comments"], 1)
        self.comment_api.denied = False
        self.comment_api.lose_post = True
        self.now += 60
        self.commenter().tick()
        self.now += 60
        self.commenter().tick()
        self.assertEqual(self.comment_api.posts, 1)
        self.assertEqual(self.db.status()["pending_comments"], 0)
        self.assertEqual(self.docker.launches, [])

    def test_comment_neutralized_when_head_changes_during_write(self):
        def advance_head():
            self.api.current[1] = pull(sha=B)

        self.comment_api.after_write = advance_head
        self.commenter().tick()
        self.assertIn(
            "Awaiting results for current commit", self.comment_api.messages[90]["body"]
        )
        self.assertIn(B, self.comment_api.messages[90]["body"])

    def test_only_own_marker_is_updated(self):
        self.comment_api.messages[12] = {"id": 12, "user": {"id": 3}, "body": MARKER}
        self.commenter().tick()
        self.assertEqual(self.comment_api.messages[12]["body"], MARKER)
        self.assertEqual(self.comment_api.posts, 1)

    def test_report_root_ancestor_symlink_is_rejected(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        reports = self.staging.job(task) / "reports"
        moved = reports.with_name("moved")
        reports.rename(moved)
        reports.symlink_to(moved)
        with self.assertRaises((OSError, ValueError)):
            collect_result(self.config, task, reports, 0)
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_docker_argv_uses_fixed_mounts_and_no_inherited_credentials(self):
        task = self.db.claim(self.now, 1800, 4, 2)
        docker = Docker(self.config)
        args = docker.create_args(task, self.staging, {"io.test.label": "test"})
        joined = " ".join(args)
        for required in (
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--sysctl=net.ipv6.conf.all.disable_ipv6=1",
            "--network=gluten-worker",
            "--user=1000:1000",
            "--cpus=16",
            "--memory=50g",
            "target=/work/driver,readonly",
            "/opt/venv/bin",
            "GLUTEN_CI_DRIVER_SHA=" + B,
        ):
            self.assertIn(required, joined)
        for forbidden in (
            "--privileged",
            "docker.sock",
            str(self.config.state_dir),
            "TOKEN",
            str(self.config.read_token_file),
            "--env-file",
            "192.0.2.8",
        ):
            self.assertNotIn(forbidden, joined)

    def test_git_checkout_objects_are_independent(self):
        root = self.config.state_dir
        original = root / "origin"
        commands = [
            ["init", str(original)],
            ["-C", str(original), "config", "user.email", "example@example.test"],
            ["-C", str(original), "config", "user.name", "CI Test"],
        ]
        for args in commands:
            subprocess.run(["/usr/bin/git", *args], check=True, capture_output=True)
        (original / "file").write_text("engine")
        subprocess.run(["/usr/bin/git", "-C", str(original), "add", "file"], check=True)
        subprocess.run(
            ["/usr/bin/git", "-C", str(original), "commit", "-m", "test"],
            check=True,
            capture_output=True,
        )
        sha = subprocess.check_output(
            ["/usr/bin/git", "-C", str(original), "rev-parse", "HEAD"], text=True
        ).strip()
        target = root / "copy"
        from unittest.mock import patch

        with patch.dict(
            os.environ,
            {
                "GIT_DIR": str(root / "unrelated"),
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "core.hooksPath",
                "GIT_CONFIG_VALUE_0": str(root),
                "GH_TOKEN": "fictional-credential",
            },
        ):
            checkout(target, original.as_uri(), sha, local=True)
        self.assertEqual((target / "file").read_text(), "engine")
        self.assertFalse((target / ".git/objects/info/alternates").exists())
        source_inodes = {
            file.stat().st_ino
            for file in (original / ".git/objects").rglob("*")
            if file.is_file()
        }
        copied_inodes = {
            file.stat().st_ino
            for file in (target / ".git/objects").rglob("*")
            if file.is_file()
        }
        self.assertTrue(source_inodes and copied_inodes)
        self.assertFalse(source_inodes & copied_inodes)

    def test_maven_cache_promotes_only_independently_verified_downloads(self):
        from io import BytesIO

        from bolt_ci.cache import MavenCache

        root = self.config.state_dir
        seed, private = root / "seed", root / "private"
        seed.mkdir()
        good = "org/example/dependency/1.2/dependency-1.2.jar"
        bad = "org/example/corrupt/2.0/corrupt-2.0.pom"
        snapshot = "org/example/new/1.0-SNAPSHOT/new-1.0-SNAPSHOT.jar"
        internal = "org/apache/gluten/bundle/1.0/bundle-1.0.jar"
        linked = "org/example/linked/1.0/linked-1.0.jar"
        for relative in (good, bad, snapshot, internal, linked):
            (private / relative).parent.mkdir(parents=True)
            if relative == linked:
                (private / relative).symlink_to(private / good)
            else:
                (private / relative).write_bytes(b"download")
        requests = []

        class Checksums:
            def open(self, request, *, timeout):
                requests.append(request.full_url)
                match = good in request.full_url
                return BytesIO(
                    hashlib.sha256(b"download" if match else b"other")
                    .hexdigest()
                    .encode()
                )

        MavenCache(seed, opener=Checksums())(private)
        self.assertEqual((seed / good).read_bytes(), b"download")
        for relative in (bad, snapshot, internal, linked):
            self.assertFalse((seed / relative).exists())
        self.assertEqual(len(requests), 2)
        self.assertTrue(
            all(
                url.startswith("https://repo.maven.apache.org/maven2/")
                for url in requests
            )
        )

    def test_config_upgrade_fails_active_attempt_without_mixing_inputs(self):
        self.runner().tick()
        config = replace(self.config, fingerprint="e" * 64)
        Runner(
            config, self.db, self.api, self.docker, self.staging, clock=lambda: self.now
        ).tick()
        self.assertEqual(len(self.docker.launches), 1)
        self.assertEqual(len(self.docker.stopped), 1)
        self.assertEqual(self.db.latest(1)["state"], "failure")

    def test_terminal_workspace_cleanup_preserves_reports_and_sealed_native_artifact(
        self,
    ):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        self.runner().tick()
        root = self.staging.job(task)
        self.assertFalse((root / "source").exists())
        self.assertFalse((root / "cache").exists())
        self.assertTrue((root / "reports/result.json").is_file())
        self.assertTrue((root / "worker.log").is_file())
        self.assertTrue((self.staging.artifact(task) / "native.tar.gz").is_file())

    def test_claim_and_queue_expiry_are_mutually_exclusive(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier

        barrier = Barrier(2)

        def claim():
            barrier.wait()
            return self.db.claim(2799, 1800, 4, 2)

        def expire():
            barrier.wait()
            self.db.expire(2800, 1800)

        with ThreadPoolExecutor(2) as executor:
            claimant = executor.submit(claim)
            expiry = executor.submit(expire)
            claimed = claimant.result()
            expiry.result()
        state = self.db.latest(1)["state"]
        if claimed:
            self.assertEqual(state, "queued")
            self.assertEqual(len(self.db.active()), 1)
        else:
            self.assertEqual(state, "queue_timeout")
            self.assertEqual(self.db.active(), [])

    def test_runtime_stop_decision_survives_scheduler_restart(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.db.mark_stopping(task, "timeout")
        # Model termination after a durable stop decision. The process
        # happened to exit zero and wrote complete evidence before exiting.
        self.complete(task)
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.assertEqual(
            self.db.task_rows(task["request_id"])[0]["category"], "timeout"
        )
        self.assertEqual(len(self.docker.launches), 1)

    def test_container_replacement_with_same_labels_is_not_accepted(self):
        self.runner().tick()
        task = self.db.active()[0]
        self.complete(task)
        name = task["container"]
        self.docker.containers[name] = replace(
            self.docker.containers[name], id="9" * 64
        )
        self.runner().tick()
        self.assertEqual(self.db.latest(1)["state"], "failure")
        self.assertEqual(len(self.docker.launches), 1)

    def test_network_receipt_binds_boot_and_live_bridge_id(self):
        boot = self.config.state_dir / "boot-id"
        boot.write_text("boot-fixture")
        identity = "b" * 64
        receipt = dict(
            schema=1,
            network=self.config.network,
            network_id=identity,
            image=self.config.image,
            boot_id="boot-fixture",
            verified_at=self.now,
            metadata_blocked=True,
            control_blocked=True,
        )
        self.config.network_preflight_file.write_text(json.dumps(receipt))

        def command(args, **kwargs):
            self.assertEqual(
                args, ["/usr/bin/docker", "network", "inspect", "gluten-worker"]
            )
            self.assertEqual(
                kwargs["env"]["DOCKER_HOST"], "unix:///var/run/docker.sock"
            )
            self.assertNotIn("GH_TOKEN", kwargs["env"])
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps([{"Id": identity, "Driver": "bridge"}]).encode(),
                b"",
            )

        docker = Docker(
            self.config, run=command, clock=lambda: self.now, boot_id_path=boot
        )
        self.assertTrue(docker.verify_network())
        self.now += self.config.network_preflight_max_age_seconds + 1
        self.assertFalse(docker.verify_network())
        self.now = receipt["verified_at"]
        boot.write_text("another-boot")
        self.assertFalse(docker.verify_network())
        boot.write_text("boot-fixture")
        receipt["network_id"] = "c" * 64
        self.config.network_preflight_file.write_text(json.dumps(receipt))
        self.assertFalse(docker.verify_network())
