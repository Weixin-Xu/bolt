# Gluten CI task library

This library provides the callable/manual `.github/workflows/bolt_backend_x86.yml`
workflow, its composite action and task helpers. It has no automatic PR or
scheduled trigger. Callers select the Bolt candidate and Apache Gluten source.

Pin the workflow and helpers to a reviewed upstream commit independently of the
candidate being tested. Publish the helper commit before adding the caller.
Each task checks out the explicit inputs, runs builds as an unprivileged user,
and uses private writable caches and report directories. The CLI requires clean
Gluten, Bolt and driver checkouts before creating commit-keyed artifacts.

## Modes

- `compile`: build native libraries and verify the exported/consumed Bolt package,
  linkage and backend output without running UT.
- `native`: compile and run native CTest, requiring nonempty test evidence.
- `full`: run 39 tasks: one native, 12 packaging and 26 Spark/JVM/connector tasks.

The Spark matrix covers 3.5, 4.0, 4.1 and 4.2; Spark 3.4 is excluded. Spark 3.5
also includes Scala 2.13 groups, local UDF, JVM/Delta and TPC checks. Connector
dependencies use explicit `bolt-ut-*` Maven profiles. Distributed shuffle,
remote storage, chaos, GPU and separate ANSI/enhanced workflows are excluded.
Native artifacts are checked against their source identity and archive digest
before downstream reuse; compile/native-only artifacts cannot replace full-mode
artifacts. Helpers do not patch Apache Gluten source.

## Validation

```bash
GLUTEN_TEST_SOURCE=/path/to/apache-gluten python3 -m unittest discover -s scripts/gluten-ci -v
```

The Apache checkout must include the prerequisites merged in Gluten #13486.
Without `GLUTEN_TEST_SOURCE`, cross-repository Spark selection tests are skipped.
Fixture tests do not establish that the full native/Spark matrix has executed.
