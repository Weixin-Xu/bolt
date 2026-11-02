# Gluten CI and runner caches

The independent `Gluten Bolt Backend` workflow compiles the tested Bolt commit
with Apache Gluten `main` for pull requests and merge groups. It resolves Gluten
once and records the exact SHA; every downstream job consumes that same source
and the native libraries from that run. It does not apply Gluten patches.

The check is informational during rollout. Do not add it to required checks or
to the existing merge-queue gate yet. Compilation failures remain visible as
failures; they are not converted into successful build results.

PRs compile native code and Spark 3.4/3.5/4.0/4.1 JVM profiles, including the
Spark 3.5 Scala 2.13 profile. `-DskipTests=true` still compiles backend test sources.
Spark SQL tests (`spark-ut`) run only on the daily schedule (02:17 Asia/Shanghai)
or a manual run with `run_tests=true`. Scheduled tests cover Spark 3.4 and 3.5,
including Scala 2.13; Apache main currently has no Bolt `spark-ut` profile for
Spark 4.x. Backend/Delta, the three ordinary Spark groups, Extended SQL and Slow
Hive run in separate jobs. Spark 3.5 also runs TPC-H/TPC-DS comparisons. There is
no Bolt-maintained testcase blacklist. Reports distinguish real failures, empty
test selections and missing native libraries.

## Time budgets

Native and all PR JVM builds share one 1,200-second compilation deadline, starting
when source resolution begins. Exceeding the deadline fails compilation instead
of silently extending it. Each scheduled test job has a 35-minute limit; the
intended execution path is at most 20 minutes of compilation plus 35 minutes of
tests and reporting. Native CTest runs in a separate 10-minute job alongside
JVM compilation, using the original compiled test executables and fixtures.
This is a budget, not a measured performance guarantee.
Cold dependency builds and runner queue time can prevent successful completion
within it. GitHub schedules and runner queues do not provide a start-time SLA.

The reference Gluten PR #2 run used Bolt `c3548fe3`: its native job took about
15 minutes and Spark 3.5 Maven compilation about 12 minutes. That run does not
prove the combined path finishes in 20 minutes. Warm the following caches before
using the short budget and measure the complete critical path on the actual
runner pool.

At least five available runner slots are needed to execute the five PR JVM
profiles concurrently after native compilation. The scheduled matrix has 19
jobs; completing its longest 35-minute shards in one wave requires 19 available
slots. Fewer slots require measuring shorter shards or accepting additional
queue time. Set `BOLT_GLUTEN_CI_RUNNER` to a JSON label array and
`BOLT_GLUTEN_CI_THREADS` to the native build parallelism appropriate to each host.

## Runner preparation

Keep compiler, C++ standard library, JDK and image identical across native and
JVM jobs. Prefer an image digest in `BOLT_GLUTEN_CI_IMAGE`. The default is the
existing `bolt-registry:5000/bolt-ci:20260921` image; the Conan service defaults to
`bolt-registry:5000/conan-server:latest` and can be overridden with
`BOLT_GLUTEN_CI_CONAN_IMAGE`. Bake `jq`, `libxml2-utils`, `unzip`, `perl` (shasum),
JDK 17, Python test dependencies and the Maven version required by Gluten into
the image to avoid repeated installation. The workflow installs missing report
tools for compatibility with existing images.

Create these directories on **every selected runner host**, outside the checkout:

```bash
sudo mkdir -p /data/ccache-data /data/gluten-ci-cache /data/gluten-ci-seed
```

The container runs as root. If the runner deployment remaps container UIDs, give
its mapped UID write access to the first two directories and read access to the
seed. Preserve the existing `/data/conan-server-data` volume as well.

| Host path | Reused content | Behavior |
| --- | --- | --- |
| `/data/ccache-data` | Bolt and Gluten C/C++ compiler results | 100 GB limit; content-based compiler identity; both compiler launchers enabled |
| `/data/gluten-ci-cache/maven/<runner>/<profile>` | Maven dependencies, Scala compiler bridge and plugins | Separate runner/profile directories prevent concurrent reactors sharing mutable snapshots |
| `/data/gluten-ci-cache/maven-tools/<runner>` | Maven distributions downloaded by `build/mvn` | Reused across checkouts |
| `/data/gluten-ci-cache/pip/<runner>` | Downloaded Python wheels | Survives job containers |
| `/data/gluten-ci-cache/spark/<key>` | Verified Spark binary and SQL resources | Key includes installer contents, exact Spark version, Scala version and resource profile; copied into each test checkout |
| `/data/gluten-ci-cache/conan-downloads` | Conan package downloads | Avoids downloading the same archives repeatedly |
| `/data/gluten-ci-seed/conan.tgz` | Prebuilt Conan dependencies and recipes | Mounted read-only and restored into a fresh job-local `CONAN_HOME` |
| `/data/conan-server-data` | Existing Conan service binaries | Retain current Bolt CI prewarming |

Do not mount one writable Conan package database into concurrent jobs. The
workflow restores package archives into fresh homes inside each runner's exclusive
temporary directory. The container path is stable across runs so dependency
include paths can reuse ccache; the database is cleared before each job. It never uploads the
candidate Bolt package to a remote or reuses an earlier candidate as the tested
binary. Only native shared libraries and their runtime dependencies are sent to
JVM jobs; large LLVM/static-engine archives are not copied to every test job.

To create the dependency seed, use an administrator-controlled container with
the **same image, profile and options as the native job**. Run the native build
once with a longer temporary budget. From that completed Bolt checkout:

```bash
# gluten-install.json is produced by the native job, including the exact graph.
conan list --graph .ci/gluten-bolt/gluten-install.json \
  --graph-binaries='*' --format=json > /tmp/gluten-packages.json
# Exclude candidate engine packages; each PR builds and exports its own engine.
jq 'with_entries(.value |= with_entries(select(.key | startswith("bolt/") | not)))' \
  /tmp/gluten-packages.json > /tmp/gluten-dependencies.json
conan cache save --list /tmp/gluten-dependencies.json --no-source \
  --file /tmp/gluten-conan-seed.tgz
sudo install -m 0644 /tmp/gluten-conan-seed.tgz /data/gluten-ci-seed/conan.tgz
```

Publish a seed only from a completed dependency build. Update it after compiler,
ABI, dependency recipe/options or image changes; keep a seed per image when
running different toolchains. If the image provides
`configure-bolt-conan-cache`, the workflow also uses that source-cache helper,
as in Gluten PR #2. Otherwise dependency sources can be rebuilt on cache misses.

Warm Maven and Spark caches on each runner/profile by running the compilation
and scheduled paths once. Mere `dependency:go-offline` does not build the Scala
compiler bridge or prime native ccache. Inspect the uploaded `ccache-stats.txt`,
generated compiler commands, dependency graph and JVM logs. Never cache a prior
check result or the entire Gluten `target/` tree as proof that a new candidate
compiled.

## Reproduction and make-only verification

Use clean worktrees and a prepared Conan toolchain. The native commands and all
options are visible in `.github/workflows/gluten-bolt.yml`; the helper can also
be sourced locally. Set `GLUTEN_DIR` when Gluten is outside the Bolt checkout.
`GLUTEN_COMPILE_DEADLINE=$(( $(date +%s) + 1200 ))` enables the same compile budget.

The existing make-only path must be distinguished from the CI's package identity
checks:

```bash
# In the exact Bolt checkout under test.
make release_spark BUILD_VERSION=ci-<bolt-sha> \
  CONAN_OVERRIDE='-o bolt/*:enable_testutil=True -o bolt/*:enable_s3=False'
make export_release BUILD_VERSION=ci-<bolt-sha>

# In Apache Gluten main, using that locally exported Bolt version.
make release-with-tests BOLT_BUILD_VERSION=ci-<bolt-sha> \
  GLUTEN_BUILD_VERSION=main ENABLE_HDFS=True ENABLE_S3=False
make jar_spark35
```

Do not run `make bolt-recipe` for a candidate build: it deletes `ep/bolt`, clones
a remote branch and exports that recipe. `test_spark35` currently ignores Maven
test failures and does not enable `spark-ut`; it is not the scheduled test
entry point. `jar_spark35` moves the compatibility `gluten-package-*.jar` name into
`output/`; the current main still produces that compatibility JAR, and this
make target passed the local verification below.

## Local validation (2026-11-02)

Verified public Bolt main `edd7850234f16bd13d1433a77f68bf21acf8262e` with Apache
Gluten main `8a8300e2b036864180452b3d058f5824b9032d7e`, without Gluten patches.
The original local working trees and Conan profiles were preserved. No cache
cleanup was needed. This host uses Clang 22, libstdc++ 16, LLD and JDK 17;
the container workflow uses the configured CI image and mold.

- Bolt engine/testutils compilation passed (1,032 Ninja steps, about 13 minutes
  at 48 parallel jobs, dependencies already available except a Paimon rebuild).
- `make release_spark` passed, followed by `make export_release` with the exact
  candidate version. The incremental make check took 37 seconds; export took 7.
- Apache Gluten `make release-with-tests` passed (201 native steps, about
  2 minutes 26 seconds, including a missing benchmark dependency build).
- Apache Gluten `make jar_spark35` passed, including backend test-source
  compilation and the final compatibility-JAR move. Maven reported 5 minutes
  13 seconds. The resulting JAR was about 120 MB.
- Eight native ObjectStore/allocator smoke tests passed. All 156 native cases
  were discovered and transferred to a different checkout. CTest reported 149
  passed, 7 skipped, 0 failures in that transferred run.
- Native libraries and runtime dependencies were archived and restored into a
  different checkout path; checksums and `ldd` validation passed.

This local compilation path is slightly over 20 minutes when its stages are
added together. It establishes source compatibility, not a successful 20-minute
CI run. Warm ccache on each runner and validate the actual five-profile critical
path in Actions. Full scheduled Spark suites have not been run locally.

The first Actions validation exposed a shared setup-action path assumption:
`bolt-build-base` wrote `global.conf` under `~/.conan2` even when `CONAN_HOME`
selected a different home. The action now honors that override, with a regression
test using an empty default home. Source identities are recorded before setup so
initialization failures also have a report artifact.
