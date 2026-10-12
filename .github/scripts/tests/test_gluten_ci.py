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

import importlib.util
import hashlib
import json
from pathlib import Path
import shutil
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "gluten_ci.py"


class GlutenCiTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), "The Gluten CI entry point is missing")
        spec = importlib.util.spec_from_file_location("gluten_ci", SCRIPT)
        self.ci = importlib.util.module_from_spec(spec)
        with patch.object(sys, "path", [str(SCRIPT.parent), *sys.path]):
            spec.loader.exec_module(self.ci)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ci.BLACKLIST = self.root / "empty-blacklist.txt"
        self.ci.BLACKLIST.write_text("# No exclusions in unrelated tests\n")

    def report(self, text):
        path = self.root / "TEST-suite.xml"
        path.write_text(text)
        return path

    def patch_fixture(self, content="OffloadDeltaScan()\n"):
        gluten = self.root / "gluten"
        gluten.mkdir()
        subprocess.run(["git", "init", "-q", str(gluten)], check=True)
        source = gluten / "component.scala"
        source.write_text(content)
        subprocess.run(["git", "-C", str(gluten), "add", "component.scala"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(gluten),
                "-c",
                "user.name=CI test",
                "-c",
                "user.email=ci-test@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        self.ci.PATCHES = self.root / "patches"
        self.ci.PATCHES.mkdir()
        patch_file = self.ci.PATCHES / "delta-api.patch"
        patch_file.write_text(
            "diff --git a/component.scala b/component.scala\n"
            "--- a/component.scala\n+++ b/component.scala\n@@ -1 +1 @@\n"
            "-OffloadDeltaScan()\n+OffloadDeltaScan(enableNativeDmlRowIndexScan = false)\n"
        )
        inputs = {
            "gluten_sha": self.ci.git(gluten, "rev-parse", "HEAD"),
            "gluten_patch_sha256": {
                patch_file.name: hashlib.sha256(patch_file.read_bytes()).hexdigest()
            },
        }
        (self.root / "inputs.json").write_text(json.dumps(inputs))
        return gluten, source, patch_file, inputs

    def test_declared_gluten_patch_is_applied_and_verified_idempotently(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        self.ci.apply_gluten_patches(gluten, self.root)
        self.assertEqual(
            source.read_text(),
            "OffloadDeltaScan(enableNativeDmlRowIndexScan = false)\n",
        )
        self.ci.verify_gluten_source(gluten, inputs)
        self.ci.apply_gluten_patches(gluten, self.root)
        evidence = json.loads((self.root / "gluten-patches.json").read_text())
        self.assertEqual(evidence["patches"][patch_file.name]["status"], "applied")

    def test_gluten_patch_already_in_main_is_recorded_without_source_changes(self):
        gluten, source, patch_file, inputs = self.patch_fixture(
            "OffloadDeltaScan(enableNativeDmlRowIndexScan = false)\n"
        )
        self.ci.apply_gluten_patches(gluten, self.root)
        self.assertEqual(self.ci.git(gluten, "diff", "HEAD"), "")
        self.ci.verify_gluten_source(gluten, inputs)
        evidence = json.loads((self.root / "gluten-patches.json").read_text())
        self.assertEqual(
            evidence["patches"][patch_file.name]["status"], "already-upstream"
        )

    def test_declared_gluten_patch_can_add_required_test_sources(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        patch_file.write_text(
            patch_file.read_text()
            + "diff --git a/SmokeSuite.scala b/SmokeSuite.scala\n"
            "new file mode 100644\n--- /dev/null\n+++ b/SmokeSuite.scala\n"
            "@@ -0,0 +1 @@\n+class SmokeSuite\n"
        )
        inputs["gluten_patch_sha256"][patch_file.name] = hashlib.sha256(
            patch_file.read_bytes()
        ).hexdigest()
        (self.root / "inputs.json").write_text(json.dumps(inputs))
        self.ci.apply_gluten_patches(gluten, self.root)
        self.assertEqual(
            (gluten / "SmokeSuite.scala").read_text(), "class SmokeSuite\n"
        )
        self.ci.verify_gluten_source(gluten, inputs)

    def test_gluten_patching_rejects_unrelated_or_changed_source(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        source.write_text(source.read_text() + "unrelated change\n")
        with self.assertRaisesRegex(ValueError, "Unrecognized Gluten source changes"):
            self.ci.apply_gluten_patches(gluten, self.root)
        source.write_text("OffloadDeltaScan()\n")
        self.ci.apply_gluten_patches(gluten, self.root)
        source.write_text(source.read_text() + "unrelated change\n")
        with self.assertRaisesRegex(ValueError, "declared patches"):
            self.ci.verify_gluten_source(gluten, inputs)

    def test_changed_patch_bundle_and_unapplicable_patch_are_fatal(self):
        gluten, source, patch_file, inputs = self.patch_fixture()
        patch_file.write_text(patch_file.read_text() + "\n")
        with self.assertRaisesRegex(ValueError, "Patch bundle differs"):
            self.ci.apply_gluten_patches(gluten, self.root)
        patch_file.write_text(patch_file.read_text().rstrip() + "\n")
        source.write_text("different API\n")
        subprocess.run(["git", "-C", str(gluten), "add", "component.scala"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(gluten),
                "-c",
                "user.name=CI test",
                "-c",
                "user.email=ci-test@example.invalid",
                "commit",
                "-qm",
                "new API",
            ],
            check=True,
        )
        inputs["gluten_sha"] = self.ci.git(gluten, "rev-parse", "HEAD")
        (self.root / "inputs.json").write_text(json.dumps(inputs))
        with self.assertRaisesRegex(
            ValueError, "neither applies nor is already upstream"
        ):
            self.ci.apply_gluten_patches(gluten, self.root)

    def test_gate_requires_the_supported_version_compilation_matrix(self):
        for result in (None, "failure", "skipped", "cancelled"):
            needs = {job: {"result": "success"} for job in ("prepare", "integration")}
            if result is not None:
                needs["compatibility"] = {"result": result}
            with (
                self.subTest(result=result),
                patch.dict("os.environ", {"GLUTEN_CI_NEEDS": json.dumps(needs)}),
            ):
                with self.assertRaisesRegex(ValueError, "compatibility"):
                    self.ci.gate()

    def test_spark4_packaging_rejects_stale_native_resources(self):
        reports = self.root / "reports"
        reports.mkdir()
        classes = self.root / "backends-bolt/target/scala-2.13/classes/linux/amd64"
        classes.mkdir(parents=True)
        (classes / "libbolt_backend.so").write_bytes(b"old candidate")
        (reports / "native-libraries.json").write_text(
            json.dumps({"libbolt_backend.so": "0" * 64})
        )
        with self.assertRaisesRegex(ValueError, "different native"):
            self.ci.verify_spark4_native(self.root, reports)

    def test_scalatest_fork_exit_must_agree_with_classified_failures(self):
        forks = self.root / "forks"
        forks.mkdir()
        record = forks / "scalatest-fork-1.json"
        for code, failures, accepted in (
            (0, [], True),
            (1, ["suite#expected"], True),
            (137, [], False),
            (-6, [], False),
            (1, [], False),
            (0, ["suite#expected"], False),
        ):
            record.write_text(json.dumps({"exit_code": code}))
            with self.subTest(code=code, failures=failures):
                if accepted:
                    self.ci.verify_scalatest_exit(forks, failures)
                else:
                    with self.assertRaisesRegex(ValueError, "ScalaTest.*exit"):
                        self.ci.verify_scalatest_exit(forks, failures)

    def test_scalatest_exit_requires_exactly_one_fresh_fork_record(self):
        forks = self.root / "forks"
        forks.mkdir()
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.ci.verify_scalatest_exit(forks, [])
        for pid in (1, 2):
            (forks / f"scalatest-fork-{pid}.json").write_text('{"exit_code": 0}')
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.ci.verify_scalatest_exit(forks, [])

    def test_jvm_wrapper_records_the_actual_runner_exit(self):
        java = self.root / "bin/java"
        java.parent.mkdir()
        java.write_text(
            "#!/usr/bin/env python3\nimport sys\nsys.exit(int(sys.argv[-1]))\n"
        )
        java.chmod(0o755)
        forks = self.root / "forks"
        with patch.dict("os.environ", {"JAVA_HOME": str(self.root)}):
            wrapper = self.ci.scalatest_jvm(forks)
        result = subprocess.run([str(wrapper), "org.scalatest.tools.Runner", "137"])
        self.assertEqual(result.returncode, 137)
        records = list(forks.glob("scalatest-fork-*.json"))
        self.assertEqual(len(records), 1)
        self.assertEqual(json.loads(records[0].read_text()), {"exit_code": 137})
        with self.assertRaisesRegex(ValueError, "ScalaTest.*exit"):
            self.ci.verify_scalatest_exit(forks, [])

    def test_local_prepare_records_host_without_an_image(self):
        with patch.dict("os.environ", {}, clear=True):
            self.ci.prepare(self.root, local=True, gluten=self.ci.ROOT)
        inputs = json.loads((self.root / "inputs.json").read_text())
        self.assertIsNone(inputs["image"])
        self.assertEqual(inputs["execution_environment"], "local-host")
        self.assertEqual(inputs["runner"], ["local"])
        self.assertEqual(
            inputs["bolt_tested_sha"], self.ci.git(self.ci.ROOT, "rev-parse", "HEAD")
        )

    def test_local_prepare_uses_the_runtime_without_a_versions_file(self):
        checkout, _, _, _ = self.patch_fixture()
        scripts = checkout / ".github/scripts"
        scripts.mkdir(parents=True)
        for name in (
            "gluten_ci.py",
            "gluten_ci.sh",
            "DiscoverGlutenSuites.java",
        ):
            shutil.copy2(SCRIPT.parent / name, scripts / name)
        (checkout / ".github/gluten-ci-blacklist.txt").touch()
        (checkout / ".github/gluten-ci-patches").mkdir()
        reports = checkout / "reports"
        result = subprocess.run(
            [
                sys.executable,
                str(scripts / "gluten_ci.py"),
                "prepare",
                "--local",
                "--gluten-dir",
                str(checkout),
                "--reports-dir",
                str(reports),
            ],
            env={},
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        inputs = json.loads((reports / "inputs.json").read_text())
        self.assertEqual(inputs["spark"], "3.5.5")
        self.assertEqual(inputs["scala"], "2.12")
        self.assertEqual(inputs["jdk"], "17")

    def test_actions_cannot_bypass_image_validation_with_local_mode(self):
        with patch.dict(
            "os.environ",
            {"GITHUB_ACTIONS": "true", "BOLT_GLUTEN_CI_IMAGE": "invalid-image"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "Local mode.*Actions"):
                self.ci.prepare(self.root, local=True)
            with self.assertRaisesRegex(ValueError, "BOLT_GLUTEN_CI_IMAGE"):
                self.ci.prepare(self.root)

    def test_prepare_reuses_the_bolt_ci_image_without_repository_variables(self):
        actual_output = subprocess.check_output

        def checkout_output(args, **kwargs):
            if args[:2] == ["git", "ls-remote"]:
                return "a" * 40 + "\trefs/heads/main\n"
            return actual_output(args, **kwargs)

        output = self.root / "actions-output"
        for image_variables in (
            {},
            {"BOLT_GLUTEN_CI_IMAGE": "", "BOLT_GLUTEN_CI_CONAN_IMAGE": ""},
            {"BOLT_GLUTEN_CI_CONAN_IMAGE": "registry.example/conan@sha256:" + "b" * 64},
        ):
            with (
                self.subTest(variables=image_variables),
                patch.dict(
                    "os.environ",
                    {
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_OUTPUT": str(output),
                        "BOLT_GLUTEN_CI_RUNNER": "",
                        **image_variables,
                    },
                    clear=True,
                ),
                patch.object(subprocess, "check_output", side_effect=checkout_output),
            ):
                self.ci.prepare(self.root)
                inputs = json.loads((self.root / "inputs.json").read_text())
                self.assertEqual(inputs["image"], "bolt-registry:5000/bolt-ci:20260114")
                self.assertEqual(inputs["runner"], ["self-hosted", "medium"])
                self.assertEqual(inputs["execution_environment"], "container")
                self.assertEqual(
                    inputs["conan_image"],
                    image_variables.get("BOLT_GLUTEN_CI_CONAN_IMAGE")
                    or "bolt-registry:5000/conan-server:latest",
                )
                self.assertIn("image=" + inputs["image"] + "\n", output.read_text())
                self.assertIn(
                    "conan_image=" + inputs["conan_image"] + "\n", output.read_text()
                )

    def test_conan_image_overrides_require_a_digest(self):
        for image in (
            "registry.example/conan:latest",
            "registry.example/conan@sha256:bad",
        ):
            with (
                self.subTest(image=image),
                patch.dict(
                    "os.environ", {"BOLT_GLUTEN_CI_CONAN_IMAGE": image}, clear=True
                ),
                patch.object(
                    subprocess,
                    "check_output",
                    return_value="a" * 40 + "\trefs/heads/main\n",
                ),
            ):
                with self.assertRaisesRegex(ValueError, "BOLT_GLUTEN_CI_CONAN_IMAGE"):
                    self.ci.prepare(self.root)

    def test_local_prepare_records_the_checked_out_gluten_source(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch.object(self.ci, "git", return_value="a" * 40),
        ):
            self.ci.prepare(self.root, local=True, gluten=self.root)
        inputs = json.loads((self.root / "inputs.json").read_text())
        self.assertEqual(inputs["gluten_sha"], "a" * 40)

    def test_prepare_resolves_apache_gluten_main_once(self):
        image = "registry.example/ci@sha256:" + "b" * 64
        actual_output = subprocess.check_output
        remote_queries = []

        def checkout_output(args, **kwargs):
            if args[:2] == ["git", "ls-remote"]:
                remote_queries.append(args)
                return "a" * 40 + "\trefs/heads/main\n"
            return actual_output(args, **kwargs)

        with (
            patch.dict("os.environ", {"BOLT_GLUTEN_CI_IMAGE": image}, clear=True),
            patch.object(subprocess, "check_output", side_effect=checkout_output),
        ):
            self.ci.prepare(self.root)
        inputs = json.loads((self.root / "inputs.json").read_text())
        self.assertEqual(inputs["gluten_repository"], "apache/gluten")
        self.assertEqual(inputs["gluten_ref"], "main")
        self.assertEqual(inputs["gluten_sha"], "a" * 40)
        self.assertEqual(inputs["image"], image)
        self.assertEqual(
            remote_queries,
            [
                [
                    "git",
                    "ls-remote",
                    "--exit-code",
                    "https://github.com/apache/gluten.git",
                    "refs/heads/main",
                ]
            ],
        )

    def test_prepare_rejects_an_unresolved_or_wrong_remote_branch(self):
        actual_output = subprocess.check_output
        for remote in ("", "a" * 40 + "\trefs/heads/other\n"):

            def checkout_output(args, **kwargs):
                return (
                    remote
                    if args[:2] == ["git", "ls-remote"]
                    else actual_output(args, **kwargs)
                )

            with (
                self.subTest(remote=remote),
                patch.dict(
                    "os.environ",
                    {"BOLT_GLUTEN_CI_IMAGE": "ci@sha256:" + "b" * 64},
                    clear=True,
                ),
                patch.object(subprocess, "check_output", side_effect=checkout_output),
                self.assertRaisesRegex(ValueError, "resolve.*branch"),
            ):
                self.ci.prepare(self.root)

    def test_reports_reject_empty_failed_and_all_skipped_runs(self):
        for text in (
            '<testsuite tests="0"/>',
            '<testsuite tests="2" skipped="2"/>',
            '<testsuite tests="2" failures="1"/>',
            '<testsuite tests="2" errors="1"/>',
            '<testsuite tests="2" skipped="-1"/>',
            '<testsuite tests="1"><testcase name="ignored"><skipped/></testcase></testsuite>',
            '<testsuite tests="1"><testcase name="bad"><failure/></testcase></testsuite>',
            '<testsuite tests="1"><testcase name="bad"><error/></testcase></testsuite>',
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.ci.verify_reports([self.report(text)])
        with self.assertRaises(ValueError):
            self.ci.verify_reports([])

    def test_reports_count_leaf_suites_once(self):
        report = self.report(
            '<testsuites tests="5"><testsuite tests="3" skipped="1">'
            "<testcase><skipped/></testcase><testcase/><testcase/></testsuite>"
            '<testsuite tests="2"><testcase/><testcase/></testsuite></testsuites>'
        )
        self.assertEqual(self.ci.verify_reports([report]), {"tests": 5, "skipped": 1})
        report = self.report(
            '<testsuite tests="2"><testcase name="ignored"><skipped/></testcase>'
            '<testcase name="executed"/></testsuite>'
        )
        self.assertEqual(self.ci.verify_reports([report]), {"tests": 2, "skipped": 1})

    def test_blacklist_snapshots_exact_entries_without_gluten_support(self):
        path = self.root / "blacklist.txt"
        path.write_text("# Known failures\norg.example.Suite#test # suffix\n")
        self.ci.BLACKLIST = path
        entries = self.ci.snapshot_blacklist(self.root / "reports")
        self.assertEqual(entries, {("org.example.Suite", "test # suffix")})
        evidence = json.loads((self.root / "reports/blacklist.json").read_text())
        self.assertEqual(evidence["entries"], 1)
        self.assertEqual(
            (self.root / "reports/blacklist.txt").read_text(), path.read_text()
        )
        for value in (
            "org.example.Suite",
            "org.example.Suite#IGNORE_ALL",
            "org.example.*#test",
            "org.example.Suite#(aborted)",
            "org.example.Suite#test\norg.example.Suite#test",
        ):
            path.write_text(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.ci.snapshot_blacklist(self.root / "invalid")
        path.write_text("org.example.Suite#another test\n")
        with self.assertRaisesRegex(ValueError, "changed between test stages"):
            self.ci.snapshot_blacklist(self.root / "reports")
        self.ci.write_json(
            self.root / "prepared/inputs.json", {"blacklist_sha256": "old"}
        )
        with self.assertRaisesRegex(ValueError, "changed after preparation"):
            self.ci.snapshot_blacklist(self.root / "prepared")

    def test_reports_classify_exact_expected_failures_without_rewriting_xml(self):
        xml = (
            '<testsuite name="org.example.Suite" tests="3" failures="1">'
            '<testcase name="known"><failure>assertion</failure></testcase>'
            '<testcase name="passing"/>'
            '<testcase name="ignored"><skipped/></testcase></testsuite>'
        )
        report = self.report(xml)
        result = self.ci.verify_reports(
            [report], blacklist={("org.example.Suite", "known")}
        )
        self.assertEqual(result["passed"], 1)
        self.assertEqual(result["expected_failures"], ["org.example.Suite#known"])
        self.assertEqual(report.read_text(), xml)
        for entries in (set(), {("org.example.OtherSuite", "known")}):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                self.ci.verify_reports([report], blacklist=entries)

    def test_blacklist_cannot_hide_errors_unidentified_failures_or_no_passes(self):
        for xml in (
            '<testsuite name="org.example.Suite" tests="2" errors="1">'
            '<testcase name="known"><error/></testcase><testcase name="good"/></testsuite>',
            '<testsuite name="org.example.Suite" tests="2" failures="1"/>',
            '<testsuite name="org.example.Suite" tests="1" failures="1">'
            '<testcase name="known"><failure/></testcase></testsuite>',
        ):
            with self.subTest(xml=xml), self.assertRaises(ValueError):
                self.ci.verify_reports(
                    [self.report(xml)], blacklist={("org.example.Suite", "known")}
                )

    def test_scalatest_requires_completion_even_when_partial_xml_passes(self):
        report = self.report(
            '<testsuite tests="1"><testcase name="passing"/></testsuite>'
        )
        summary = (
            "Run completed in 1 second.\nTotal number of tests run: 1\n"
            "Suites: completed 1, aborted 0\n"
            "Tests: succeeded 1, failed 0, canceled 0, ignored 0, pending 0\n"
        )
        for log in (
            "Starting suite...\nJVM exited with code 137\n",
            summary.replace("Run completed", "Run aborted"),
            summary.replace("aborted 0", "aborted 1"),
            summary + summary,
            summary.replace("succeeded 1", "succeeded 2"),
        ):
            with self.subTest(log=log), self.assertRaises(ValueError):
                self.ci.verify_scalatest_run(log, [report])
        self.ci.verify_scalatest_run(summary, [report])

    def test_xml_counts_cannot_invent_a_passing_test(self):
        for xml in (
            '<testsuite tests="1"/>',
            '<testsuite tests="2"><testcase><skipped/></testcase></testsuite>',
            '<testsuite tests="3" failures="1"><testcase name="known"><failure/></testcase>'
            '<testcase name="good"/></testsuite>',
        ):
            with self.subTest(xml=xml), self.assertRaises(ValueError):
                self.ci.verify_reports(
                    [self.report(xml)], blacklist={("org.example.Suite", "known")}
                )

    def test_junit_passes_cannot_hide_all_skipped_scalatest(self):
        junit = self.root / "TEST-junit.xml"
        junit.write_text(
            '<testsuite xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xsi:noNamespaceSchemaLocation="surefire-test-report-3.0.xsd" tests="1">'
            "<testcase/></testsuite>"
        )
        scala = self.report(
            '<testsuite tests="1"><testcase><skipped/></testcase></testsuite>'
        )
        log = (
            "Run completed in 1 second.\nTotal number of tests run: 0\n"
            "Suites: completed 1, aborted 0\n"
            "Tests: succeeded 0, failed 0, canceled 0, ignored 1, pending 0\n"
        )
        self.ci.verify_reports([junit, scala], blacklist=set())
        with self.assertRaises(ValueError):
            self.ci.verify_scalatest_run(log, [junit, scala])
        scala.write_text('<testsuite tests="1"><testcase/></testsuite>')
        self.ci.verify_scalatest_run(
            log.replace("tests run: 0", "tests run: 1")
            .replace("succeeded 0", "succeeded 1")
            .replace("ignored 1", "ignored 0"),
            [junit, scala],
        )

    @unittest.skipUnless(shutil.which("javac") and shutil.which("java"), "Requires JDK")
    def test_discovery_preserves_unusual_packages_without_initializing_suites(self):
        version = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT
        ).decode()
        if int(re.search(r'version "(?:1\.)?(\d+)', version)[1]) < 17:
            self.skipTest("Java source-mode discovery requires JDK 17")
        sources = {
            "org/scalatest/Suite.java": "package org.scalatest; public abstract class Suite {}",
            "org/scalatest/DoNotDiscover.java": (
                "package org.scalatest; import java.lang.annotation.*; "
                "@Retention(RetentionPolicy.RUNTIME) public @interface DoNotDiscover {}"
            ),
            "org/scalatest/WrapWith.java": (
                "package org.scalatest; import java.lang.annotation.*; "
                "@Inherited @Retention(RetentionPolicy.RUNTIME) public @interface WrapWith "
                "{ Class<? extends Suite> value(); }"
            ),
            "future/FutureSuite.java": (
                "package future; public class FutureSuite extends org.scalatest.Suite {"
                'static { if(System.nanoTime()>0) throw new RuntimeException("initialized"); } }'
            ),
            "future/HiddenSuite.java": (
                "package future; @org.scalatest.DoNotDiscover "
                "public class HiddenSuite extends org.scalatest.Suite {}"
            ),
            "future/AbstractSuite.java": (
                "package future; public abstract class AbstractSuite extends org.scalatest.Suite {}"
            ),
            "future/PrivateSuite.java": (
                "package future; public class PrivateSuite extends org.scalatest.Suite "
                "{ private PrivateSuite() {} }"
            ),
            "future/Wrapper.java": (
                "package future; public class Wrapper extends org.scalatest.Suite "
                "{ public Wrapper(Class<?> type) {} }"
            ),
            "future/WrappedSuite.java": (
                "package future; @org.scalatest.WrapWith(future.Wrapper.class) "
                "public class WrappedSuite {}"
            ),
            "future/InheritedWrappedSuite.java": (
                "package future; public class InheritedWrappedSuite extends WrappedSuite {}"
            ),
            "future/VisibleSuite.java": (
                "package future; public class VisibleSuite extends HiddenSuite {}"
            ),
            "future/AbstractWrappedSuite.java": (
                "package future; @org.scalatest.WrapWith(future.Wrapper.class) "
                "abstract class AbstractWrappedSuite {}"
            ),
        }
        paths = []
        for name, code in sources.items():
            path = self.root / "sources" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(code)
            paths.append(str(path))
        classes = self.root / "classes"
        classes.mkdir()
        subprocess.run(
            ["javac", "-d", str(classes), *paths], check=True, capture_output=True
        )
        names = self.root / "suites.txt"
        subprocess.run(
            [
                "java",
                "--class-path",
                str(classes),
                str(SCRIPT.parent / "DiscoverGlutenSuites.java"),
                str(names),
                str(classes),
                str(classes),
            ],
            check=True,
            capture_output=True,
        )
        self.assertEqual(
            names.read_text().splitlines(),
            [
                "future.AbstractWrappedSuite",
                "future.FutureSuite",
                "future.InheritedWrappedSuite",
                "future.VisibleSuite",
                "future.WrappedSuite",
            ],
        )

    def test_gate_fails_for_missing_skipped_cancelled_and_failed_jobs(self):
        for needs in (
            {},
            {"prepare": {"result": "success"}},
            {"prepare": {"result": "success"}, "integration": {"result": "skipped"}},
            {"prepare": {"result": "success"}, "integration": {"result": "cancelled"}},
            {"prepare": {"result": "success"}, "integration": {"result": "failure"}},
        ):
            with self.subTest(needs=needs):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPT),
                        "gate",
                        "--reports-dir",
                        str(self.root),
                    ],
                    env={"GLUTEN_CI_NEEDS": json.dumps(needs)},
                    capture_output=True,
                )
                self.assertNotEqual(result.returncode, 0)
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "gate", "--reports-dir", str(self.root)],
            env={
                "GLUTEN_CI_NEEDS": json.dumps(
                    {
                        "prepare": {"result": "success"},
                        "compatibility": {"result": "success"},
                        "integration": {"result": "success"},
                    }
                )
            },
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_package_rejects_revision_or_abi_drift(self):
        node = {
            "name": "bolt",
            "ref": "bolt/ci-source#recipe",
            "rrev": "recipe",
            "package_id": "id",
            "prev": "package",
            "context": "host",
            "settings": {
                "os": "Linux",
                "arch": "x86_64",
                "compiler": "gcc",
                "compiler.version": "12",
                "compiler.libcxx": "libstdc++11",
                "build_type": "Release",
            },
            "options": {
                "spark_compatible": "True",
                "enable_testutil": "True",
                "enable_hdfs": "False",
                "enable_s3": "False",
                "enable_asan": "False",
            },
        }
        node["binary"] = "Cache"
        exported = {"graph": {"nodes": {"0": node}}}
        self.ci.verify_package(exported, exported)
        for key, value in (
            ("binary", "Build"),
            ("prev", "wrong"),
            ("package_id", "wrong"),
            ("rrev", None),
            ("context", "build"),
        ):
            changed = dict(node, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.ci.verify_package(exported, {"graph": {"nodes": {"1": changed}}})
        for options in (
            dict(node["options"], spark_compatible="False"),
            dict(node["options"], enable_testutil="False"),
        ):
            with self.assertRaises(ValueError):
                bad = {"graph": {"nodes": {"0": dict(node, options=options)}}}
                self.ci.verify_package(bad, bad)

    def test_dependency_graph_allows_build_completion_but_rejects_drift(self):
        node = {
            "ref": "zlib/1.3.1#recipe",
            "context": "host",
            "settings": {"compiler.version": "12"},
            "options": {"shared": "False"},
            "package_id": "id",
            "binary": "Build",
        }
        graph = {"graph": {"nodes": {"0": {}, "1": node}, "error": None}}
        installed = dict(node, binary="Cache", prev="new-package")
        actual = {"graph": {"nodes": {"0": {}, "99": installed}, "error": None}}
        self.ci.verify_dependency_graph(graph, actual)
        for key, value in (
            ("ref", "zlib/1.3.1#different-recipe"),
            ("context", "build"),
            ("package_id", "different-id"),
            ("settings", {"compiler.version": "13"}),
            ("options", {"shared": "True"}),
        ):
            with (
                self.subTest(key=key),
                self.assertRaisesRegex(ValueError, "graph.*changed"),
            ):
                self.ci.verify_dependency_graph(
                    graph,
                    {"graph": {"nodes": {"1": dict(installed, **{key: value})}}},
                )
        with self.assertRaisesRegex(ValueError, "graph.*error"):
            self.ci.verify_dependency_graph(
                graph,
                {"graph": {"nodes": {"1": installed}, "error": {"type": "conflict"}}},
            )

    def test_dependency_graph_recovers_options_removed_by_package_info(self):
        options = {"shared": "False", "plasma": "deprecated"}
        node = {
            "ref": "arrow/15.0.1-oss#recipe",
            "context": "host",
            "settings": {"compiler.version": "12"},
            "options": options,
            "info": {"options": dict(options)},
            "package_id": "same-binary",
        }
        before = {"graph": {"nodes": {"0": {}, "1": node}}}
        installed = dict(node, options={"shared": "False"})
        after = {"graph": {"nodes": {"0": {}, "1": installed}}}
        self.ci.verify_dependency_graph(before, after)
        for update in (
            {"options": {"shared": "True"}},
            {"info": {"options": {"shared": "False", "plasma": "True"}}},
        ):
            with (
                self.subTest(update=update),
                self.assertRaisesRegex(ValueError, "graph.*changed"),
            ):
                self.ci.verify_dependency_graph(
                    before,
                    {"graph": {"nodes": {"0": {}, "1": dict(installed, **update)}}},
                )

    def test_tpc_launcher_loads_gluten_before_spark_arrow(self):
        library = self.root / "tools/gluten-it/package/target/lib"
        library.mkdir(parents=True)
        bundle = library / "gluten-package-1.8.0-SNAPSHOT.jar"
        bundle.touch()
        metadata = library / "gluten-package-1.8.0-SNAPSHOT-3.5.jar"
        metadata.touch()
        (library / "arrow-vector-12.0.1.jar").touch()
        script = self.root / "tools/gluten-it/sbin/gluten-it.sh"
        script.parent.mkdir()
        script.write_text(
            f"LIB_DIR={shlex.quote(str(library))}\n\n"
            "if false; then\n  exit 1\nfi\n\n"
            "JAR_PATH=$LIB_DIR/*\n\n"
            "SPARK_JVM_OPTIONS=$($JAVA_HOME/bin/java -cp $JAR_PATH "
            "org.apache.gluten.integration.SparkJvmOptions)\n\n"
            "EMBEDDED_SPARK_HOME=$BASEDIR/../spark-home\n\n"
            "printf '%s\\n' \"$JAR_PATH\"\n"
        )
        java = self.root / "jdk/bin/java"
        java.parent.mkdir(parents=True)
        java.write_text("#!/bin/sh\nexit 0\n")
        java.chmod(0o755)
        subprocess.run(
            [
                "git",
                "apply",
                "--allow-empty",
                "--include=tools/gluten-it/sbin/gluten-it.sh",
                str(self.ci.PATCHES / "003-bolt-delta-tpch-fixture.patch"),
            ],
            cwd=self.root,
            check=True,
        )
        classpath = subprocess.check_output(
            ["bash", str(script)], env={"JAVA_HOME": str(java.parents[1])}, text=True
        ).strip()
        entries = classpath.split(":")
        self.assertEqual(set(entries[:-1]), {str(bundle), str(metadata)})
        self.assertEqual(entries[-1], f"{library}/*")

    def test_tpc_rejects_baseline_only_native_and_missing_queries(self):
        log = (
            "Running baseline query q1 (iteration 0)...\n"
            "== Physical Plan ==\nHashAggregateTransformer\n"
            "Running test query q1 (iteration 0)...\n"
            "== Physical Plan ==\nHashAggregate\n"
            "Successfully ran query q1. Returned row count: 1\n"
            "Summary: 1 out of 1 queries passed.\n"
        )
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(log, {"q1"})
        native = log.replace("\nHashAggregate\n", "\nHashAggregateTransformer\n")
        self.assertEqual(self.ci.verify_tpc(native, {"q1"})["native_queries"], ["q1"])
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(native, {"q1", "q6"})
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(native.replace("1 out of 1", "0 out of 1"), {"q1"})

    def test_tpc_requires_native_nodes_in_executed_final_aqe_plan(self):
        log = (
            "Running test query q1 (iteration 0)...\n== Physical Plan ==\n"
            "AdaptiveSparkPlan isFinalPlan=true\n== Final Plan ==\nHashAggregate\n"
            "== Initial Plan ==\nHashAggregateTransformer\n"
            "Successfully ran query q1. Returned row count: 1\n"
            "Summary: 1 out of 1 queries passed.\n"
        )
        with self.assertRaises(ValueError):
            self.ci.verify_tpc(log, {"q1"})
        final = log.replace(
            "== Final Plan ==\nHashAggregate\n",
            "== Final Plan ==\nHashAggregateTransformer\n",
        )
        self.assertEqual(self.ci.verify_tpc(final, {"q1"})["native_queries"], ["q1"])

    def test_spark_environment_requires_binary_and_source_test_resources(self):
        home = self.root / "spark_home"
        with self.assertRaises(ValueError):
            self.ci.validate_spark_home(home)
        for path in (
            home / "bin/spark-submit",
            home / "jars/spark-core_2.12-3.5.5.jar",
            home / "python/lib/pyspark.zip",
            home / "python/lib/py4j-0.10.9.7-src.zip",
            home / "sql/core/src/test/resources/sql-tests/inputs/query.sql",
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        submit = home / "bin/spark-submit"
        submit.chmod(0o755)
        (home / "RELEASE").write_text("Spark 3.5.5 built for Hadoop 3\n")
        self.ci.validate_spark_home(home)
        for name in (
            "jars/spark-core_2.12-3.5.5.jar",
            "python/lib/pyspark.zip",
            "python/lib/py4j-0.10.9.7-src.zip",
            "sql/core/src/test/resources/sql-tests/inputs/query.sql",
        ):
            path = home / name
            hidden = self.root / "missing"
            path.rename(hidden)
            try:
                with self.subTest(missing=name), self.assertRaises(ValueError):
                    self.ci.validate_spark_home(home)
            finally:
                hidden.rename(path)
        submit.chmod(0o644)
        with self.assertRaises(ValueError):
            self.ci.validate_spark_home(home)
        submit.chmod(0o755)
        (home / "RELEASE").write_text("Spark 4.0.0 built for Hadoop 3\n")
        with self.assertRaises(ValueError):
            self.ci.validate_spark_home(home)

    def test_spark_environment_accepts_versioned_pyspark_distribution(self):
        home = self.root / "pyspark_home"
        for name in (
            "bin/spark-submit",
            "jars/spark-core_2.12-3.5.5.jar",
            "python/lib/py4j-0.10.9.7-src.zip",
            "sql/core/src/test/resources/sql-tests/inputs/query.sql",
        ):
            path = home / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        (home / "bin/spark-submit").chmod(0o755)
        with zipfile.ZipFile(home / "python/lib/pyspark.zip", "w") as archive:
            archive.writestr("pyspark/version.py", "__version__: str = '3.5.5'\n")
        self.ci.validate_spark_home(home)
        with zipfile.ZipFile(home / "python/lib/pyspark.zip", "w") as archive:
            archive.writestr("pyspark/version.py", "__version__ = '4.0.0'\n")
        with self.assertRaises(ValueError):
            self.ci.validate_spark_home(home)

    def test_binary_jar_is_verified_even_when_sources_jar_is_present(self):
        import hashlib

        target = self.root / "package/target"
        target.mkdir(parents=True)
        (target / "gluten-package-1.0-sources.jar").touch()
        (self.root / "native-libraries.json").write_text(
            json.dumps({"libbolt_backend.so": hashlib.sha256(b"candidate").hexdigest()})
        )
        jar_path = target / "gluten-package-1.0.jar"

        def package(provider, native):
            with zipfile.ZipFile(jar_path, "w") as jar:
                jar.writestr("META-INF/gluten-components/" + provider, "")
                jar.writestr("linux/amd64/libbolt_backend.so", native)

        package("org.apache.gluten.backendsapi.bolt.BoltBackend", b"candidate")
        self.ci.verify_jar(self.root, self.root)
        package("org.apache.gluten.backendsapi.bolt.BoltBackend", b"remote-main")
        with self.assertRaises(ValueError):
            self.ci.verify_jar(self.root, self.root)
        package("org.apache.gluten.backendsapi.velox.VeloxBackend", b"candidate")
        with self.assertRaises(ValueError):
            self.ci.verify_jar(self.root, self.root)

    def test_binary_jar_accepts_only_metadata_spark_classifier(self):
        import hashlib

        target = self.root / "package/target"
        target.mkdir(parents=True)
        (self.root / "native-libraries.json").write_text(
            json.dumps({"libbolt_backend.so": hashlib.sha256(b"candidate").hexdigest()})
        )
        bundle = target / "gluten-package-1.0.jar"
        with zipfile.ZipFile(bundle, "w") as jar:
            jar.writestr(
                "META-INF/gluten-components/org.apache.gluten.backendsapi.bolt.BoltBackend",
                "",
            )
            jar.writestr("linux/amd64/libbolt_backend.so", b"candidate")
        classifier = target / "gluten-package-1.0-3.5.jar"
        with zipfile.ZipFile(classifier, "w") as jar:
            jar.writestr("gluten-build-info.properties", "spark.version=3.5.5")
            jar.writestr(
                "META-INF/maven/org.apache.gluten/gluten-package/pom.properties", ""
            )
            for name in ("LICENSE", "NOTICE", "DEPENDENCIES"):
                jar.writestr("META-INF/" + name, "Apache packaging metadata")
        self.assertEqual(self.ci.verify_jar(self.root, self.root), bundle)
        with zipfile.ZipFile(classifier, "a") as jar:
            jar.writestr("org/apache/gluten/Stale.class", b"stale")
        with self.assertRaisesRegex(ValueError, "classifier.*metadata"):
            self.ci.verify_jar(self.root, self.root)


class GlutenCiShellTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / ".github/scripts"
        scripts.mkdir(parents=True)
        for source in SCRIPT.parent.glob("gluten_ci.*"):
            shutil.copy2(source, scripts / source.name)
        (self.root / ".github/gluten-ci-blacklist.txt").write_text(
            "org.example.NativeSuite#known failure\n"
        )
        self.gluten = self.root / "gluten"
        activation = self.gluten / "cpp/build/Release/generators/conanrun.sh"
        activation.parent.mkdir(parents=True)
        activation.write_text("true\n")
        self.java = self.root / "jdk/bin/java"
        self.java.parent.mkdir(parents=True)
        self.java.write_text('#!/bin/sh\nexit "${FAKE_FORK_EXIT:-0}"\n')
        self.java.chmod(0o755)
        self.maven = self.gluten / "fake-maven.py"
        self.maven.write_text(
            "import os, pathlib, subprocess, sys\n"
            "from xml.sax.saxutils import escape\n"
            "report = pathlib.Path('gluten-ut/spark35/target/test-reports/TEST-suite.xml')\n"
            "report.parent.mkdir(parents=True, exist_ok=True)\n"
            "name = os.environ.get('FAKE_FAILURE', '')\n"
            "failed = 1 if name else 0\n"
            'body = \'<testcase name="pass" classname="org.example.NativeSuite"/>\'\n'
            "if failed:\n"
            "    body += '<testcase name=\"' + escape(name) + '\" classname=\"org.example.NativeSuite\"><failure/></testcase>'\n"
            "report.write_text('<testsuite name=\"org.example.NativeSuite\" tests=\"' + str(1+failed) + '\" failures=\"' + str(failed) + '\">' + body + '</testsuite>')\n"
            "jvm = next(arg.split('=',1)[1] for arg in sys.argv if arg.startswith('-Djvm='))\n"
            "subprocess.run([jvm, 'org.scalatest.tools.Runner'], check=False)\n"
            "print('Run completed in 1 second.\\nTotal number of tests run: ' + str(1+failed) + '\\nSuites: completed 1, aborted 0\\nTests: succeeded 1, failed ' + str(failed) + ', canceled 0, ignored 0, pending 0')\n"
            "sys.exit(int(os.environ.get('FAKE_MAVEN_EXIT', '0')))\n"
        )

    def run_stage(self, **overrides):
        import os

        env = {
            **os.environ,
            "JAVA_HOME": str(self.java.parent.parent),
            "GLUTEN_DIR": str(self.gluten),
            **overrides,
        }
        return subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; ci_init; status=0; ci_maven_test spark-2 gluten-ut/spark35 "$2" "$3" || status=$?; exit "$status"',
                "test",
                str(self.root / ".github/scripts/gluten_ci.sh"),
                sys.executable,
                str(self.maven),
            ],
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
        )

    def test_shell_runner_records_verified_reports(self):
        result = self.run_stage()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = self.root / ".ci/gluten-bolt/results.json"
        self.assertEqual(json.loads(report.read_text())["spark-2"]["passed"], 1)

    def test_shell_runner_accepts_only_exact_registered_failures(self):
        result = self.run_stage(FAKE_FAILURE="known failure", FAKE_FORK_EXIT="1")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        result = self.run_stage(FAKE_FAILURE="new failure", FAKE_FORK_EXIT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unlisted failed test", result.stdout + result.stderr)

    def test_shell_runner_rejects_crash_even_with_complete_passing_xml(self):
        result = self.run_stage(FAKE_FORK_EXIT="137")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fork exit", result.stdout + result.stderr)

    def test_shell_runner_preserves_reports_after_maven_failure(self):
        result = self.run_stage(FAKE_MAVEN_EXIT="9")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(
            list((self.root / ".ci/gluten-bolt/spark-2").glob("*TEST-*.xml"))
        )

    def test_later_group_cannot_hide_an_earlier_failed_group(self):
        result = self.run_stage(
            CI_GROUP="first", FAKE_FAILURE="new failure", FAKE_FORK_EXIT="1"
        )
        self.assertNotEqual(result.returncode, 0)
        result = self.run_stage(CI_GROUP="second")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads((self.root / ".ci/gluten-bolt/results.json").read_text())
        self.assertEqual(report["spark-2"]["result"], "failure")
        self.assertEqual(report["spark-2"]["groups"]["first"]["result"], "failure")

    def test_profile_change_lock_prevents_test_execution(self):
        import time

        lock = self.gluten / ".ci/gluten-bolt-profile.lock"
        lock.parent.mkdir()
        marker = self.root / "locked"
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from pathlib import Path; import fcntl, sys, time; "
                "lock = open(sys.argv[1], 'w'); fcntl.flock(lock, fcntl.LOCK_EX); "
                "Path(sys.argv[2]).touch(); time.sleep(30)",
                str(lock),
                str(marker),
            ]
        )
        try:
            deadline = time.monotonic() + 5
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(marker.exists())
            result = self.run_stage()
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(
                (self.gluten / "gluten-ut/spark35/target/test-reports").exists()
            )
        finally:
            holder.terminate()
            holder.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
