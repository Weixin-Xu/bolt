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

"""Validate Gluten CI inputs, candidate artifacts and completed test runs."""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SPARK_VERSION = "3.5.5"
SCALA_VERSION = "2.12"
JDK_VERSION = "17"
SPARK_PROFILE = SPARK_VERSION.rsplit(".", 1)[0]
BLACKLIST = ROOT / ".github/gluten-ci-blacklist.txt"
PATCHES = ROOT / ".github/gluten-ci-patches"
SHA = re.compile(r"[0-9a-f]{40}")
IMAGE = re.compile(r"[a-zA-Z0-9._:/-]+@sha256:[0-9a-f]{64}")
DEFAULT_IMAGE = "bolt-registry:5000/bolt-ci:20260114"
DEFAULT_CONAN_IMAGE = "bolt-registry:5000/conan-server:latest"
STAGES = (
    "native",
    "jvm",
    "spark-1",
    "spark-2",
    "spark-3",
    "extended",
    "slow-hive",
    "tpc-h",
    "tpc-ds",
    "spark40",
    "spark41",
)


def write_atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path, value):
    write_atomic(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def verify_reports(paths, blacklist=None):
    if not paths:
        raise ValueError("Required test XML reports are missing")
    tests = skipped = 0
    expected_failures = []
    for path in paths:
        suites = ET.parse(path).getroot().iter("testsuite")
        for suite in suites:
            if suite.findall("testsuite"):
                continue
            counts = {
                key: int(suite.get(key, "0"))
                for key in ("tests", "skipped", "failures", "errors")
            }
            if any(value < 0 for value in counts.values()):
                raise ValueError(f"Invalid test counts in {path}")
            cases = suite.findall("testcase")
            if len(cases) != counts["tests"]:
                raise ValueError(
                    f"XML test count does not match testcase discovery in {path}"
                )
            # ScalaTest emits <skipped/> for ignored/canceled tests without
            # adding a suite-level skipped attribute. Count the actual cases.
            counts["skipped"] = max(
                counts["skipped"],
                sum(case.find("skipped") is not None for case in cases),
            )
            if (
                any(value < 0 for value in counts.values())
                or counts["skipped"] > counts["tests"]
            ):
                raise ValueError(f"Invalid test counts in {path}")
            failed_cases = [case for case in cases if case.find("failure") is not None]
            if counts["errors"] or any(
                case.find("error") is not None for case in cases
            ):
                raise ValueError(f"Failed tests in {path}")
            if counts["failures"] > len(failed_cases):
                raise ValueError(f"Unidentified test failures in {path}")
            for case in failed_cases:
                key = (case.get("classname") or suite.get("name"), case.get("name"))
                if blacklist is None or key not in blacklist:
                    raise ValueError(f"Unlisted failed test in {path}: {key}")
                expected_failures.append("#".join(key))
            if counts["skipped"] + len(failed_cases) > counts["tests"]:
                raise ValueError(f"Invalid test counts in {path}")
            tests += counts["tests"]
            skipped += counts["skipped"]
    if tests <= skipped + len(expected_failures):
        raise ValueError(
            "No passing tests (empty discovery, all skipped or all expected failures)"
        )
    result = {"tests": tests, "skipped": skipped}
    if blacklist is not None:
        result.update(
            passed=tests - skipped - len(expected_failures),
            expected_failures=sorted(expected_failures),
        )
    return result


def verify_scalatest_run(log, paths):
    # With testFailureIgnore, ScalaTest's Maven plugin also swallows an
    # abnormal fork exit. Passing partial XML is insufficient without the
    # final runner summary and its executed-test counts.
    log = re.sub(r"\x1b\[[0-9;]*m", "", log)
    summary = re.findall(
        r"^Run completed in [^\n]+\nTotal number of tests run: (\d+)\n"
        r"Suites: completed (\d+), aborted (\d+)\n"
        r"Tests: succeeded (\d+), failed (\d+), canceled (\d+), ignored (\d+), pending (\d+)\s*$",
        log,
        re.MULTILINE,
    )
    if len(summary) != 1 or "*** RUN ABORTED ***" in log:
        raise ValueError("ScalaTest fork did not complete exactly one test run")
    total, suites, aborted, passed, failed, canceled, ignored, pending = map(
        int, summary[0]
    )
    if aborted or pending or not suites or not passed or total != passed + failed:
        raise ValueError("ScalaTest run aborted or has inconsistent completion counts")
    cases = []
    for path in paths:
        root = ET.parse(path).getroot()
        # Surefire writes its own versioned XML; it is already validated by
        # verify_reports and must not inflate ScalaTest's completion counts.
        if "surefire-test-report" in root.get(
            "{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation", ""
        ):
            continue
        for suite in root.iter("testsuite"):
            cases.extend(suite.findall("testcase"))
    xml_skipped = sum(case.find("skipped") is not None for case in cases)
    xml_failed = sum(case.find("failure") is not None for case in cases)
    xml_passed = len(cases) - xml_skipped - xml_failed
    # ScalaTest can omit some ignored base-suite cases from XML; executed
    # passes/failures must still match exactly.
    if xml_passed != passed or xml_failed != failed or xml_skipped > canceled + ignored:
        raise ValueError("ScalaTest completion counts do not match its XML reports")


def bolt_node(graph):
    nodes = [
        node
        for node in graph["graph"]["nodes"].values()
        if node.get("name") == "bolt" and node.get("context") == "host"
    ]
    if len(nodes) != 1:
        raise ValueError("Expected exactly one host Bolt package in the Conan graph")
    node = nodes[0]
    for key in ("ref", "rrev", "package_id", "prev"):
        if not node.get(key):
            raise ValueError(f"Missing Bolt package identity: {key}")
    for key in ("spark_compatible", "enable_testutil"):
        if node["options"].get(key) != "True":
            raise ValueError(f"Bolt package must enable {key}")
    if (
        node["settings"].get("os") != "Linux"
        or node["settings"].get("arch") != "x86_64"
    ):
        raise ValueError("First-stage CI requires Linux x86_64")
    return node


def verify_package(exported, consumed):
    expected, actual = bolt_node(exported), bolt_node(consumed)
    if actual.get("binary") != "Cache":
        raise ValueError(
            "Gluten must consume the locally exported Bolt binary without building or downloading it"
        )
    for key in ("ref", "rrev", "package_id", "prev", "settings", "options"):
        if expected[key] != actual[key]:
            raise ValueError(f"Gluten consumed a different Bolt package: {key}")
    return {
        key: actual[key]
        for key in ("ref", "rrev", "package_id", "prev", "settings", "options")
    }


def verify_dependency_graph(expected, actual):
    """Check recipe/ABI identity in every context after dependency builds."""
    fields = ("ref", "context", "settings", "options", "package_id")

    def identity(node):
        result = {key: node.get(key) for key in fields}
        # package_info() can remove deprecated options after package lookup.
        # Recover only missing keys from the frozen binary configuration;
        # live option values still win, so actual configuration drift fails.
        frozen = (node.get("info") or {}).get("options") or {}
        result["options"] = {**frozen, **(node.get("options") or {})}
        return json.dumps(result, sort_keys=True)

    def identities(graph):
        if graph["graph"].get("error"):
            raise ValueError(
                f"Conan dependency graph has an error: {graph['graph']['error']}"
            )
        return Counter(
            identity(node)
            for node_id, node in graph["graph"]["nodes"].items()
            if node_id != "0"
        )

    before, after = identities(expected), identities(actual)
    if before != after:
        raise ValueError(
            "Conan dependency graph changed between preflight and installation"
        )
    return {
        "dependency_nodes": sum(after.values()),
        "dependency_graph_sha256": hashlib.sha256(
            json.dumps(sorted(after.elements())).encode()
        ).hexdigest(),
    }


def verify_tpc(log, expected):
    summaries = re.findall(r"Summary: (\d+) out of (\d+) queries passed\.", log)
    if summaries != [(str(len(expected)), str(len(expected)))]:
        raise ValueError("TPC comparison is missing, empty, incomplete or failed")
    blocks = re.split(r"Running test query (\w+) \(iteration 0\)\.\.\.", log)
    plans = {}
    for query, block in zip(blocks[1::2], blocks[2::2]):
        if query in plans or f"Successfully ran query {query}." not in block:
            raise ValueError(f"Duplicate or unsuccessful TPC query: {query}")
        physical = block.split("== Physical Plan ==", 1)
        if len(physical) != 2:
            raise ValueError(f"No executed physical plan for {query}")
        executed = physical[1].split("Successfully ran query", 1)[0]
        if "AdaptiveSparkPlan" in executed:
            if "isFinalPlan=true" not in executed or "== Final Plan ==" not in executed:
                raise ValueError(f"No final AQE execution plan for {query}")
            executed = executed.split("== Final Plan ==", 1)[1].split(
                "== Initial Plan ==", 1
            )[0]
        plans[query] = bool(re.search(r"\b\w+Transformer\b", executed))
    if set(plans) != expected:
        raise ValueError("TPC query discovery does not match the complete benchmark")
    native = sorted(query for query, used_native in plans.items() if used_native)
    if not native:
        raise ValueError("All TPC queries fell back to vanilla Spark")
    return {
        "native_queries": native,
        "fully_fallback_queries": sorted(expected - set(native)),
    }


def prepare(reports, local=False, gluten=None):
    if local and os.environ.get("GITHUB_ACTIONS") == "true":
        raise ValueError("Local mode cannot run in GitHub Actions")
    config = {
        "gluten_repository": "apache/gluten",
        "gluten_ref": "main",
        "spark": SPARK_VERSION,
        "scala": SCALA_VERSION,
        "jdk": JDK_VERSION,
        "os": "Linux",
        "architecture": "x86_64",
        "build_type": "Release",
    }
    repository = config["gluten_repository"]
    branch = config["gluten_ref"]
    image = None if local else os.environ.get("BOLT_GLUTEN_CI_IMAGE") or DEFAULT_IMAGE
    if not local and image != DEFAULT_IMAGE and not IMAGE.fullmatch(image):
        raise ValueError(
            "BOLT_GLUTEN_CI_IMAGE must use the Bolt CI image or an image@sha256:digest"
        )
    conan_image = (
        None
        if local
        else os.environ.get("BOLT_GLUTEN_CI_CONAN_IMAGE") or DEFAULT_CONAN_IMAGE
    )
    if (
        not local
        and conan_image != DEFAULT_CONAN_IMAGE
        and not IMAGE.fullmatch(conan_image)
    ):
        raise ValueError(
            "BOLT_GLUTEN_CI_CONAN_IMAGE overrides require an image@sha256:digest"
        )
    runner = (
        ["local"]
        if local
        else json.loads(
            os.environ.get("BOLT_GLUTEN_CI_RUNNER") or '["self-hosted", "medium"]'
        )
    )
    if (
        not isinstance(runner, list)
        or not runner
        or any(
            not isinstance(label, str) or not re.fullmatch(r"[\w.-]+", label)
            for label in runner
        )
    ):
        raise ValueError("BOLT_GLUTEN_CI_RUNNER must be a JSON array of runner labels")
    if local and gluten is not None:
        config["gluten_sha"] = git(gluten, "rev-parse", "HEAD")
    else:
        ref = "refs/heads/" + branch
        remote = subprocess.check_output(
            [
                "git",
                "ls-remote",
                "--exit-code",
                f"https://github.com/{repository}.git",
                ref,
            ],
            text=True,
        ).splitlines()
        if (
            len(remote) != 1
            or len(remote[0].split()) != 2
            or remote[0].split()[1] != ref
            or not SHA.fullmatch(remote[0].split()[0])
        ):
            raise ValueError(f"Cannot resolve Gluten branch {repository}:{branch}")
        config["gluten_sha"] = remote[0].split()[0]
    tested = os.environ.get("GITHUB_SHA") or git(ROOT, "rev-parse", "HEAD")
    if tested != git(ROOT, "rev-parse", "HEAD"):
        raise ValueError("Bolt checkout differs from the candidate merge SHA")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event = json.loads(Path(event_path).read_text()) if event_path else {}
    provenance = {
        **config,
        "bolt_repository": os.environ.get("GITHUB_REPOSITORY", "bytedance/bolt"),
        "bolt_ref": os.environ.get("GITHUB_REF", "local"),
        "bolt_tested_sha": tested,
        "bolt_pr_head_sha": event.get("pull_request", {}).get("head", {}).get("sha"),
        "merge_group_head_sha": event.get("merge_group", {}).get("head_sha"),
        "event": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "image": image,
        "conan_image": conan_image,
        "execution_environment": "local-host" if local else "container",
        "runner": runner,
        "required_stages": STAGES,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "shell_script_sha256": hashlib.sha256(
            (ROOT / ".github/scripts/gluten_ci.sh").read_bytes()
        ).hexdigest(),
        "discovery_script_sha256": hashlib.sha256(
            (ROOT / ".github/scripts/DiscoverGlutenSuites.java").read_bytes()
        ).hexdigest(),
        "blacklist_sha256": hashlib.sha256(BLACKLIST.read_bytes()).hexdigest(),
        "gluten_patch_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(PATCHES.glob("*.patch"))
        },
    }
    write_json(reports / "inputs.json", provenance)
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as stream:
            for key, value in {
                "gluten_repository": repository,
                "gluten_sha": config["gluten_sha"],
                "image": image,
                "conan_image": conan_image,
                "runner": json.dumps(runner),
            }.items():
                stream.write(f"{key}={value}\n")


def gluten_patch_tree(gluten, inputs):
    """Derive the only accepted source tree from main and the declared patches."""
    if git(gluten, "rev-parse", "HEAD") != inputs["gluten_sha"]:
        raise ValueError("Gluten checkout differs from the resolved Apache main")
    patches = sorted(PATCHES.glob("*.patch"))
    digests = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in patches
    }
    if digests != inputs["gluten_patch_sha256"]:
        raise ValueError("Patch bundle differs from the prepared CI inputs")
    records = {}
    with tempfile.TemporaryDirectory(prefix="gluten-patch-index-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        command = ["git", "-C", str(gluten)]
        subprocess.run([*command, "read-tree", "HEAD"], env=env, check=True)
        for path in patches:
            apply = [*command, "apply", "--cached"]
            check = subprocess.run(
                [*apply, "--check", str(path)], env=env, capture_output=True
            )
            if check.returncode == 0:
                subprocess.run([*apply, str(path)], env=env, check=True)
                status = "applied"
            elif (
                subprocess.run(
                    [*apply, "--reverse", "--check", str(path)],
                    env=env,
                    capture_output=True,
                ).returncode
                == 0
            ):
                status = "already-upstream"
            else:
                raise ValueError(
                    f"Gluten patch neither applies nor is already upstream: {path.name}"
                )
            records[path.name] = {"status": status, "sha256": digests[path.name]}
        tree = subprocess.check_output(
            [*command, "write-tree"], env=env, text=True
        ).strip()
    return tree, records


def verify_gluten_source(gluten, inputs):
    tree, records = gluten_patch_tree(gluten, inputs)
    if git(gluten, "diff", tree, "--"):
        raise ValueError("Gluten source differs from Apache main plus declared patches")
    return tree, records


def apply_gluten_patches(gluten, reports):
    inputs = json.loads((reports / "inputs.json").read_text())
    tree, records = gluten_patch_tree(gluten, inputs)
    if git(gluten, "diff", tree, "--"):
        if git(gluten, "diff", "HEAD", "--"):
            raise ValueError(
                "Unrecognized Gluten source changes before patch application"
            )
        for name, record in records.items():
            if record["status"] == "applied":
                subprocess.run(
                    ["git", "-C", str(gluten), "apply", "--index", str(PATCHES / name)],
                    check=True,
                )
    verify_gluten_source(gluten, inputs)
    write_json(
        reports / "gluten-patches.json",
        {"base_sha": inputs["gluten_sha"], "source_tree": tree, "patches": records},
    )
    for name, record in records.items():
        print(f"Gluten compatibility patch {name}: {record['status']}", flush=True)


def java_executable(env):
    java_home = env.get("JAVA_HOME")
    java = (
        Path(java_home) / "bin/java"
        if java_home
        else shutil.which("java", path=env.get("PATH", os.defpath))
    )
    if not java or not Path(java).is_file():
        raise ValueError("Gluten tests require a Java executable")
    return str(Path(java).resolve())


def scalatest_jvm(forks):
    """Capture the real Runner exit that ScalaTest's failure-ignore flag hides."""
    java = java_executable(os.environ)
    if forks.exists():
        shutil.rmtree(forks)
    forks.mkdir(parents=True)
    # Surefire also consumes -Djvm and requires the executable's bin/java suffix.
    wrapper = forks / "bin/java"
    wrapper.parent.mkdir()
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import json, os, signal, subprocess, sys\n"
        "from pathlib import Path\n"
        f"child = subprocess.Popen([{java!r}, *sys.argv[1:]])\n"
        "def forward(signum, frame):\n"
        "    if child.poll() is None:\n"
        "        child.send_signal(signum)\n"
        "for signum in (signal.SIGINT, signal.SIGTERM):\n"
        "    signal.signal(signum, forward)\n"
        "code = child.wait()\n"
        "if 'org.scalatest.tools.Runner' in sys.argv[1:]:\n"
        f"    record = Path({str(forks.resolve())!r}) / f'scalatest-fork-{{os.getpid()}}.json'\n"
        "    record.write_text(json.dumps({'exit_code': code}) + '\\n')\n"
        "sys.exit(code if code >= 0 else 128 - code)\n"
    )
    wrapper.chmod(0o755)
    return wrapper.resolve()


def verify_scalatest_exit(forks, expected_failures):
    records = list(forks.glob("scalatest-fork-*.json"))
    if len(records) != 1:
        raise ValueError("ScalaTest requires exactly one fresh fork exit record")
    code = json.loads(records[0].read_text()).get("exit_code")
    expected = 1 if expected_failures else 0
    if type(code) is not int or code != expected:
        raise ValueError(f"ScalaTest fork exit {code!r} does not match test results")


def gluten_package_jars(directory):
    jars = []
    for path in directory.glob("gluten-package-*.jar"):
        if path.name.endswith(
            ("-sources.jar", "-test-sources.jar", "-javadoc.jar", "-tests.jar")
        ):
            continue
        # Maven also attaches a small Spark classifier JAR with build metadata
        # and Apache license files. The unclassified artifact is the native bundle.
        if path.name.endswith("-3.5.jar"):
            with zipfile.ZipFile(path) as jar:
                for name in jar.namelist():
                    if not (
                        name.endswith("/")
                        or name
                        in (
                            "META-INF/MANIFEST.MF",
                            "META-INF/LICENSE",
                            "META-INF/NOTICE",
                            "META-INF/DEPENDENCIES",
                            "gluten-build-info.properties",
                        )
                        or (
                            name.startswith("META-INF/maven/")
                            and name.endswith(("/pom.xml", "/pom.properties"))
                        )
                    ):
                        raise ValueError(
                            "Spark classifier must contain only build/Maven/license "
                            f"metadata: {name}"
                        )
            continue
        jars.append(path)
    return jars


def verify_jar(gluten, reports):
    jars = gluten_package_jars(gluten / "package/target")
    if len(jars) != 1:
        raise ValueError("Expected exactly one Gluten package JAR")
    expected = json.loads((reports / "native-libraries.json").read_text())
    with zipfile.ZipFile(jars[0]) as jar:
        providers = [
            name.removeprefix("META-INF/gluten-components/")
            for name in jar.namelist()
            if name.startswith("META-INF/gluten-components/")
            and "backendsapi." in name
            and name.endswith("Backend")
        ]
        if providers != ["org.apache.gluten.backendsapi.bolt.BoltBackend"]:
            raise ValueError("Packaged JAR does not select only the Bolt backend")
        for name, digest in expected.items():
            entries = [entry for entry in jar.namelist() if entry.endswith("/" + name)]
            if (
                len(entries) != 1
                or hashlib.sha256(jar.read(entries[0])).hexdigest() != digest
            ):
                raise ValueError(
                    f"Packaged JAR contains a different native library: {name}"
                )
    return jars[0]


def snapshot_blacklist(reports):
    content = BLACKLIST.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    inputs = reports / "inputs.json"
    if (
        inputs.is_file()
        and json.loads(inputs.read_text()).get("blacklist_sha256") != digest
    ):
        raise ValueError("Blacklist changed after preparation")
    snapshot = reports / "blacklist.txt"
    if snapshot.is_file() and snapshot.read_bytes() != content:
        raise ValueError("Blacklist changed between test stages")
    entries = set()
    for number, line in enumerate(content.decode().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        suite, separator, name = line.partition("#")
        if (
            not separator
            or not re.fullmatch(r"[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)+", suite)
            or not name.strip()
            or name in ("IGNORE_ALL", "(aborted)")
            or (suite, name) in entries
        ):
            raise ValueError(
                f"Invalid or duplicate exact blacklist entry at line {number}"
            )
        entries.add((suite, name))
    write_json(
        reports / "blacklist.json",
        {"entries": len(entries), "sha256": digest},
    )
    write_atomic(snapshot, content)
    return entries


def verify_spark4_native(gluten, reports):
    expected = json.loads((reports / "native-libraries.json").read_text())
    classes = gluten / "backends-bolt/target/scala-2.13/classes"
    for name, digest in expected.items():
        paths = list(classes.rglob(name))
        if (
            len(paths) != 1
            or hashlib.sha256(paths[0].read_bytes()).hexdigest() != digest
        ):
            raise ValueError(
                f"Spark 4 backend contains a different native library: {name}"
            )


def gate():
    needs = json.loads(os.environ["GLUTEN_CI_NEEDS"])
    for job in ("prepare", "compatibility", "integration"):
        if needs.get(job, {}).get("result") != "success":
            raise ValueError(f"Required job {job} did not succeed: {needs.get(job)}")
    print("All required Gluten Bolt integration jobs succeeded")


def validate_spark_home(home):
    release = home / "RELEASE"
    if release.is_file():
        version_matches = bool(
            re.search(rf"\bSpark {re.escape(SPARK_VERSION)}\b", release.read_text())
        )
    else:
        try:
            with zipfile.ZipFile(home / "python/lib/pyspark.zip") as archive:
                version_matches = bool(
                    re.search(
                        rf"__version__(?:\s*:\s*str)?\s*=\s*['\"]{re.escape(SPARK_VERSION)}['\"]",
                        archive.read("pyspark/version.py").decode(),
                    )
                )
        except (OSError, KeyError, zipfile.BadZipFile):
            version_matches = False
    if (
        not version_matches
        or not os.access(home / "bin/spark-submit", os.X_OK)
        or not (home / f"jars/spark-core_{SCALA_VERSION}-{SPARK_VERSION}.jar").is_file()
        or not (home / "python/lib/pyspark.zip").is_file()
        or not list((home / "python/lib").glob("py4j-*-src.zip"))
    ):
        raise ValueError(f"Spark {SPARK_VERSION} binary distribution is incomplete")
    inputs = home / "sql/core/src/test/resources/sql-tests/inputs"
    if not inputs.is_dir() or not any(inputs.iterdir()):
        raise ValueError("Matching Spark SQL test resources are missing")


def configuration(reports):
    values = {
        "SPARK_VERSION": SPARK_VERSION,
        "SPARK_PROFILE": SPARK_PROFILE,
        "SCALA_VERSION": SCALA_VERSION,
        "JDK_VERSION": JDK_VERSION,
    }
    if (reports / "inputs.json").is_file():
        inputs = json.loads((reports / "inputs.json").read_text())
        for name, key in (
            ("BOLT_BUILD_VERSION", "bolt_tested_sha"),
            ("GLUTEN_BUILD_VERSION", "gluten_sha"),
        ):
            if not SHA.fullmatch(inputs[key]):
                raise ValueError(f"Invalid source identity: {key}")
            values[name] = "ci-" + inputs[key]
    print("\n".join(f"{key}={shlex.quote(value)}" for key, value in values.items()))


def verify_candidate(gluten, reports):
    inputs = json.loads((reports / "inputs.json").read_text())
    if git(ROOT, "rev-parse", "HEAD") != inputs["bolt_tested_sha"] or git(
        ROOT, "diff", "HEAD", "--"
    ):
        raise ValueError("Bolt checkout differs from the unmodified candidate")
    verify_gluten_source(gluten, inputs)
    index = (
        Path(os.environ.get("CONAN_HOME", str(Path.home() / ".conan2")))
        / "conan-center-index"
    )
    if index.is_dir():
        inputs["conan_recipe_index_sha"] = git(index, "rev-parse", "HEAD")
        write_json(reports / "inputs.json", inputs)


def verify_arrow_source(reports):
    installed = json.loads((reports / "gluten-install.json").read_text())
    arrows = [
        node
        for node in installed["graph"]["nodes"].values()
        if node.get("name") == "arrow" and node.get("context") == "host"
    ]
    if len(arrows) != 1 or not arrows[0]["ref"].startswith("arrow/15.0.1-oss#"):
        raise ValueError("The Arrow Java helper requires arrow/15.0.1-oss")
    ref = arrows[0]["ref"]
    source = (
        Path(
            subprocess.check_output(
                ["conan", "cache", "path", ref, "--folder=source"], text=True
            ).strip()
        )
        / "src"
    )
    recipe = subprocess.check_output(["conan", "cache", "path", ref], text=True).strip()
    helper = subprocess.check_output(
        ["conan", "cache", "path", "arrow/15.0.1-oss"], text=True
    ).strip()
    if not source.is_dir() or recipe != helper:
        raise ValueError("Arrow helper source does not match the installed recipe")
    write_json(reports / "arrow-source.json", {"ref": ref, "source": str(source)})


def verify_native(gluten, reports):
    native = gluten / "cpp/build/releases"
    packaged = gluten / "cpp/build/package/bolt/linux/amd64"
    for name in ("libgluten.so", "libbolt_backend.so", "libglutenlibloader.so"):
        if not (native / name).is_file() or not (packaged / name).is_file():
            raise ValueError(f"Missing native library: {name}")
        if (native / name).read_bytes() != (packaged / name).read_bytes():
            raise ValueError(f"Native staging contains a different library: {name}")
    write_json(
        reports / "native-libraries.json",
        {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in packaged.glob("*.so")
        },
    )


def verify_stage(args, gluten, reports):
    output = reports / args.stage
    if args.group:
        output /= args.group
    log = output / "run.log"
    if args.stage.startswith("tpc-"):
        kind = "h" if args.stage == "tpc-h" else "ds"
        queries = {
            p.stem
            for p in (
                gluten / f"tools/gluten-it/common/src/main/resources/tpc{kind}-queries"
            ).glob("*.sql")
        }
        if not queries:
            raise ValueError("TPC query resources are missing")
        evidence = verify_tpc(log.read_text(), queries)
    else:
        paths = sorted(output.glob("*TEST-*.xml"))
        blacklist = (
            None
            if args.stage in ("native", "spark40", "spark41")
            else snapshot_blacklist(reports)
        )
        evidence = verify_reports(paths, blacklist)
        if args.suites:
            selected = set(args.suites.split(","))
            reported = {
                suite.get("name")
                for path in paths
                for suite in ET.parse(path).getroot().iter("testsuite")
            }
            if not selected.issubset(reported):
                raise ValueError("Missing discovered backend suite reports")
        if args.stage != "native":
            verify_scalatest_run(log.read_text(), paths)
            verify_scalatest_exit(
                output / "forks", evidence.get("expected_failures", [])
            )
    record_stage(reports, args.stage, {"result": "success", **evidence}, args.group)


def record_stage(reports, stage, evidence, group=""):
    path = reports / "results.json"
    results = json.loads(path.read_text()) if path.is_file() else {}
    if group:
        groups = results.get(stage, {}).get("groups", {})
        groups[group] = evidence
        evidence = {
            "groups": groups,
            "result": "success"
            if all(g["result"] == "success" for g in groups.values())
            else "failure",
            "tests": sum(g.get("tests", 0) for g in groups.values()),
            "skipped": sum(g.get("skipped", 0) for g in groups.values()),
            "expected_failures": sorted(
                f for g in groups.values() for f in g.get("expected_failures", [])
            ),
        }
        evidence["passed"] = (
            evidence["tests"] - evidence["skipped"] - len(evidence["expected_failures"])
        )
    results[stage] = evidence
    write_json(path, results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "prepare",
            "patch",
            "config",
            "source",
            "package",
            "arrow",
            "native",
            "spark4-native",
            "spark-home",
            "jar",
            "runner",
            "verify",
            "failed",
            "reset",
            "gate",
        ),
    )
    parser.add_argument("stage", nargs="?", choices=STAGES)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--gluten-dir", type=Path, default=ROOT / "gluten")
    parser.add_argument("--reports-dir", type=Path, default=ROOT / ".ci/gluten-bolt")
    parser.add_argument("--consumed", type=Path)
    parser.add_argument("--graph", type=Path)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--group", default="")
    parser.add_argument("--suites", default="")
    parser.add_argument("--tpc", action="store_true")
    args = parser.parse_args()
    if args.local and args.command != "prepare":
        parser.error("--local applies only to prepare")
    if args.command in ("verify", "failed", "reset") and not args.stage:
        parser.error("A test stage is required")
    if args.group and not re.fullmatch(r"[\w.$-]+", args.group):
        parser.error("Invalid report group")
    reports, gluten = args.reports_dir.resolve(), args.gluten_dir.resolve()
    reports.mkdir(parents=True, exist_ok=True)
    try:
        if args.command == "prepare":
            prepare(reports, local=args.local, gluten=gluten if args.local else None)
        elif args.command == "patch":
            apply_gluten_patches(gluten, reports)
        elif args.command == "config":
            configuration(reports)
        elif args.command == "source":
            verify_candidate(gluten, reports)
        elif args.command == "package":
            if args.consumed is None:
                parser.error("--consumed is required")
            actual = json.loads(args.consumed.read_text())
            identity = verify_package(
                json.loads((reports / "bolt-export.json").read_text()), actual
            )
            write_json(reports / "bolt-package.json", identity)
            if args.graph:
                write_json(
                    reports / "conan-dependencies.json",
                    verify_dependency_graph(json.loads(args.graph.read_text()), actual),
                )
        elif args.command == "arrow":
            verify_arrow_source(reports)
        elif args.command == "native":
            verify_native(gluten, reports)
        elif args.command == "spark4-native":
            verify_spark4_native(gluten, reports)
        elif args.command == "spark-home":
            if args.directory is None:
                parser.error("--directory is required")
            validate_spark_home(args.directory)
        elif args.command == "jar":
            candidate = verify_jar(gluten, reports)
            if args.tpc:
                jars = gluten_package_jars(
                    gluten / "tools/gluten-it/package/target/lib"
                )
                if len(jars) != 1 or jars[0].read_bytes() != candidate.read_bytes():
                    raise ValueError(
                        "TPC runtime does not contain the candidate Gluten package"
                    )
        elif args.command == "runner":
            if args.directory is None:
                parser.error("--directory is required")
            print(scalatest_jvm(args.directory))
        elif args.command == "verify":
            verify_stage(args, gluten, reports)
        elif args.command == "reset":
            path = reports / "results.json"
            results = json.loads(path.read_text()) if path.is_file() else {}
            results.pop(args.stage, None)
            write_json(path, results)
        elif args.command == "failed":
            raise ValueError("Test command exited unsuccessfully; see the stage log")
        else:
            gate()
    except (
        ValueError,
        OSError,
        KeyError,
        subprocess.CalledProcessError,
        ET.ParseError,
    ) as error:
        write_json(reports / f"{args.command}-error.json", {"error": str(error)})
        if args.stage:
            record_stage(
                reports,
                args.stage,
                {"result": "failure", "error": str(error)},
                args.group,
            )
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
