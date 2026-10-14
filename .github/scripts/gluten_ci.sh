#!/usr/bin/env bash
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

# Source this file from workflow steps or a local shell. Test selection and
# build commands live in gluten-bolt.yml; this helper preserves their evidence.
set -euo pipefail

BOLT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GLUTEN_DIR="${GLUTEN_DIR:-${BOLT_ROOT}/gluten}"
GLUTEN_REPORTS_DIR="${GLUTEN_REPORTS_DIR:-${BOLT_ROOT}/.ci/gluten-bolt}"

ci_apply_patches() {
  local patch
  for patch in "${BOLT_ROOT}"/.github/gluten-ci-patches/*.patch; do
    if git -C "${GLUTEN_DIR}" apply --check "${patch}"; then
      git -C "${GLUTEN_DIR}" apply --index "${patch}"
    elif git -C "${GLUTEN_DIR}" apply --reverse --check "${patch}"; then
      echo "Already applied: ${patch##*/}"
    else
      echo "Cannot apply patch: ${patch}" >&2
      return 1
    fi
  done
}

ci_set_maven() {
  MVN=("${GLUTEN_DIR}/build/mvn" -ntp -fae -Pfast-build -Pbackends-bolt
    "-Pspark-${SPARK_PROFILE}" "-Pscala-${SCALA_VERSION}" "-Pjava-${JDK_VERSION}"
    "-Dmaven.compiler.release=${JDK_VERSION}"
    -DskipTests=false -Dmaven.test.skip=false
    -Dmaven.test.failure.ignore=false -DfailIfNoTests=false)
  case "${CI_BUILD:-spark35}" in
    spark34) MVN+=(-Piceberg -Piceberg-test -Pdelta -Phudi -Ppaimon) ;;
    spark34-slow | spark35) MVN+=(-Piceberg -Pdelta -Phudi -Ppaimon) ;;
    spark35-scala213) MVN+=(-Piceberg -Pdelta) ;;
    spark40) MVN+=(-Pdelta -Ppaimon) ;;
    spark40-hive | spark41) MVN+=(-Pdelta) ;;
    spark41-slow) ;;
  esac
}

ci_init() {
  GLUTEN_DIR="$(cd "${GLUTEN_DIR}" && pwd)"
  mkdir -p "${GLUTEN_REPORTS_DIR}"
  GLUTEN_REPORTS_DIR="$(cd "${GLUTEN_REPORTS_DIR}" && pwd)"
  GLUTEN_MAVEN_REPO="${GLUTEN_MAVEN_REPO:-${HOME}/.m2/repository}"
  export MAVEN_ARGS="${MAVEN_ARGS:-} -Dmaven.repo.local=${GLUTEN_MAVEN_REPO}"
  SCALA_VERSION=2.12
  case "${CI_BUILD:-spark35}" in
    spark34 | spark34-slow) SPARK_PROFILE=3.4 ;;
    spark35) SPARK_PROFILE=3.5 ;;
    spark35-scala213)
      SPARK_PROFILE=3.5
      SCALA_VERSION=2.13
      ;;
    spark40 | spark40-hive)
      SPARK_PROFILE=4.0
      SCALA_VERSION=2.13
      ;;
    spark41 | spark41-slow)
      SPARK_PROFILE=4.1
      SCALA_VERSION=2.13
      ;;
    *)
      echo "Unknown CI build profile: ${CI_BUILD}" >&2
      return 1
      ;;
  esac
  SPARK_MODULE="spark${SPARK_PROFILE/./}"
  SPARK_VERSION="$(xmllint --nonet --xpath \
    "string(/*[local-name()='project']/*[local-name()='profiles']/*[local-name()='profile'][*[local-name()='id']='spark-${SPARK_PROFILE}']/*[local-name()='properties']/*[local-name()='spark.version'])" \
    "${GLUTEN_DIR}/pom.xml")"
  [[ -n "${SPARK_VERSION}" ]]
  SPARK_RESOURCE_PROFILE="${SPARK_PROFILE}"
  SPARK_RESOURCE_MODULE="${SPARK_MODULE}"
  if [[ "${CI_BUILD:-spark35}" == spark35-scala213 ]]; then
    SPARK_RESOURCE_PROFILE=3.5-scala2.13
    SPARK_RESOURCE_MODULE=spark35-scala-2.13
  fi
  JDK_VERSION=17
  ci_set_maven
  export SPARK_TESTING=true SPARK_SCALA_VERSION="${SCALA_VERSION}" SPARK_ANSI_SQL_MODE=false
  export GLUTEN_TEST_BLACKLIST="${GLUTEN_TEST_BLACKLIST:-${BOLT_ROOT}/.github/gluten-ci-blacklist.txt}"
}

ci_conan_options() {
  options=(-o "bolt/*:spark_compatible=True" -o "bolt/*:enable_testutil=True"
    -o "bolt/*:shared=False" -o "bolt/*:enable_hdfs=True"
    -o "bolt/*:enable_s3=False" -o "bolt/*:enable_asan=False")
  gluten_options=(-o "gluten/*:shared=True" -o "gluten/*:build_tests=True"
    -o "gluten/*:enable_hdfs=True" -o "gluten/*:enable_s3=False"
    -o "gluten/*:enable_asan=False")
  profile=(-pr:h default)
  if [[ -f "${BOLT_ROOT}/scripts/conan/bolt.profile" ]]; then
    profile+=(-pr:h "${BOLT_ROOT}/scripts/conan/bolt.profile")
  fi
}

ci_verify_bolt_package() {
  # Gluten must consume the exact exported Bolt binary, without rebuilding it.
  jq -e --slurpfile exported "${GLUTEN_REPORTS_DIR}/bolt-export.json" '
    [.graph.nodes[] | select(.name == "bolt" and .context == "host")] as $actual |
    [$exported[0].graph.nodes[] | select(.name == "bolt" and .context == "host")] as $expected |
    ($actual | length) == 1 and ($expected | length) == 1 and
    $actual[0].binary == "Cache" and
    ($actual[0] | {ref, package_id, prev}) == ($expected[0] | {ref, package_id, prev})
  ' "${GLUTEN_REPORTS_DIR}/gluten-install.json"
}

ci_restore_native() {
  export BOLT_BUILD_VERSION="ci-$(git -C "${BOLT_ROOT}" rev-parse HEAD)"
  local gluten_build_version="ci-$(git -C "${GLUTEN_DIR}" rev-parse HEAD)"
  ci_conan_options
  conan cache restore "${GLUTEN_REPORTS_DIR}/bolt.tgz"
  mkdir -p "${GLUTEN_MAVEN_REPO}"
  tar -C "${GLUTEN_MAVEN_REPO}" -xzf "${GLUTEN_REPORTS_DIR}/arrow.tgz"
  tar -C "${GLUTEN_DIR}" -xzf "${GLUTEN_REPORTS_DIR}/native.tgz"
  # Regenerate runtime paths for this job and verify the exact candidate package.
  (cd "${GLUTEN_DIR}/cpp" && conan install . --name=gluten "--version=${gluten_build_version}" \
    "${profile[@]}" -s build_type=Release "${options[@]}" "${gluten_options[@]}" \
    --lockfile= --build=missing "--build=~bolt/*" --format=json) \
    > "${GLUTEN_REPORTS_DIR}/gluten-install.json"
  ci_verify_bolt_package
  (cd "${GLUTEN_DIR}/cpp/build/package/bolt/linux/amd64" \
    && sha256sum -c "${GLUTEN_REPORTS_DIR}/native.sha256")
  for library in "${GLUTEN_DIR}"/cpp/build/package/bolt/linux/amd64/*.so; do
    ci_runtime ldd "${library}"
  done | tee "${GLUTEN_REPORTS_DIR}/native-linkage.log"
  if grep -q 'not found' "${GLUTEN_REPORTS_DIR}/native-linkage.log"; then return 1; fi
  rm "${GLUTEN_REPORTS_DIR}/bolt.tgz" "${GLUTEN_REPORTS_DIR}/arrow.tgz" \
    "${GLUTEN_REPORTS_DIR}/native.tgz"
}

ci_pack_jvm() (
  (cd "${GLUTEN_DIR}/backends-bolt/target/scala-${SCALA_VERSION}/classes/linux/amd64" \
    && sha256sum -c "${GLUTEN_REPORTS_DIR}/native.sha256")
  tar -C "${GLUTEN_MAVEN_REPO}" -I 'gzip -1' -cf "${GLUTEN_REPORTS_DIR}/jvm-maven.tgz" \
    org/apache/gluten
  cd "${GLUTEN_DIR}"
  targets=()
  while IFS= read -r pom; do
    target="${pom%pom.xml}target"
    [[ ! -d "${target}" ]] || targets+=("${target}")
  done < <(git ls-files '**/pom.xml' pom.xml)
  # Installed backend jars are already in the Maven archive; retain nested test resources.
  tar --no-wildcards-match-slash --exclude='backends-bolt/target/*.jar' -I 'gzip -1' \
    -cf "${GLUTEN_REPORTS_DIR}/jvm-tests.tgz" "${targets[@]}"
)

ci_restore_jvm() {
  tar -C "${GLUTEN_MAVEN_REPO}" -xzf "${GLUTEN_REPORTS_DIR}/jvm-maven.tgz"
  tar -C "${GLUTEN_DIR}" -xzf "${GLUTEN_REPORTS_DIR}/jvm-tests.tgz"
  rm "${GLUTEN_REPORTS_DIR}/jvm-maven.tgz" "${GLUTEN_REPORTS_DIR}/jvm-tests.tgz"
  (cd "${GLUTEN_DIR}/backends-bolt/target/scala-${SCALA_VERSION}/classes/linux/amd64" \
    && sha256sum -c "${GLUTEN_REPORTS_DIR}/native.sha256")
}

ci_run() {
  local log="$1"
  shift
  mkdir -p "$(dirname "${log}")" || return
  printf '%q ' "$@" | tee -a "${log}" || return
  printf '\n' | tee -a "${log}" || return
  "$@" 2>&1 | tee -a "${log}"
}

ci_runtime() (
  source "${GLUTEN_DIR}/cpp/build/Release/generators/conanrun.sh" || return
  "$@"
)

ci_profile_lock() {
  mkdir -p "${GLUTEN_DIR}/.ci" || return
  exec 9> "${GLUTEN_DIR}/.ci/gluten-bolt-profile.lock" || return
  flock --nonblock "--${1}" 9 || return
  CI_PROFILE_LOCKED=1
}

ci_spark_environment() {
  BOLT_GLUTEN_SPARK_HOME="${BOLT_GLUTEN_SPARK_HOME:-/opt/shims/${SPARK_RESOURCE_MODULE}/spark_home}"
  test -x "${BOLT_GLUTEN_SPARK_HOME}/bin/spark-submit"
  test -f "${BOLT_GLUTEN_SPARK_HOME}/jars/spark-core_${SCALA_VERSION}-${SPARK_VERSION}.jar"
  test -d "${BOLT_GLUTEN_SPARK_HOME}/sql/core/src/test/resources"
  export SPARK_HOME="${BOLT_GLUTEN_SPARK_HOME}"
  local archives=("${SPARK_HOME}"/python/lib/*.zip)
  test -f "${archives[0]}"
  local zipped
  zipped="$(
    IFS=:
    printf '%s' "${archives[*]}"
  )"
  export PYTHONPATH="${zipped}${PYTHONPATH:+:${PYTHONPATH}}"
}

# Select whole suites for independent jobs; ScalaTest remains sequential within a JVM.
ci_select_sql_shard() {
  local prefixes="${1:-}" shard="${CI_TEST_SHARD:-1}" shards="${CI_TEST_SHARDS:-1}"
  [[ "${shard}" =~ ^[1-9][0-9]*$ && "${shards}" =~ ^[1-9][0-9]*$ ]]
  ((shard <= shards))
  [[ "${shards}" != 1 ]] || return 0
  local output="${GLUTEN_REPORTS_DIR}/${CI_TEST_STAGE}/shard-${shard}"
  local classes="${GLUTEN_DIR}/gluten-ut/${SPARK_MODULE}/target/scala-${SCALA_VERSION}"
  mkdir -p "${output}"
  (cd "${GLUTEN_DIR}" && ci_run "${output}/discovery.log" ci_runtime \
    "${MVN[@]}" dependency:build-classpath -Pspark-ut \
    -pl "backends-bolt,gluten-ut/${SPARK_MODULE}" -DincludeScope=test \
    "-Dmdep.outputFile=${output}/test-classpath.txt")
  "${JAVA_HOME}/bin/java" --class-path \
    "${classes}/test-classes:${classes}/classes:$(cat "${output}/test-classpath.txt")" \
    "${BOLT_ROOT}/.github/scripts/DiscoverGlutenSuites.java" \
    "${output}/discovered-suites.txt" "${classes}/test-classes"
  local suite prefix index=0 assigned
  local selected=() patterns=()
  IFS=, read -r -a patterns <<< "${prefixes}"
  : > "${output}/group-suites.txt"
  while IFS= read -r suite; do
    if [[ "${CI_TEST_STAGE}" == extended ]]; then
      # Most discovered suites have no Extended SQL tests. Keep the long query
      # suite separate, and distribute the remaining suites across other jobs.
      if [[ "${suite}" == org.apache.spark.sql.GlutenSQLQueryTestSuite ]]; then
        assigned=1
      else
        assigned=$((index % (shards - 1) + 2))
        index=$((index + 1))
      fi
    else
      assigned=0
      for prefix in "${patterns[@]}"; do
        [[ "${suite}" == "${prefix}"* ]] || continue
        assigned=$((index % shards + 1))
        index=$((index + 1))
        break
      done
      [[ "${assigned}" != 0 ]] || continue
    fi
    printf '%s\n' "${suite}" >> "${output}/group-suites.txt"
    [[ "${assigned}" != "${shard}" ]] || selected+=("${suite}")
  done < "${output}/discovered-suites.txt"
  ((${#selected[@]} > 0))
  printf '%s\n' "${selected[@]}" > "${output}/selected-suites.txt"
  CI_SUITES="$(
    IFS=,
    printf '%s' "${selected[*]}"
  )"
  CI_GROUP="shard-${shard}"
}

ci_maven_test() (
  local stage="$1" module="$2"
  shift 2
  local output="${GLUTEN_REPORTS_DIR}/${stage}${CI_GROUP:+/${CI_GROUP}}"
  local lock_module=spark-ut
  [[ "${stage}" != jvm ]] || lock_module=backends-bolt
  [[ "${CI_PROFILE_LOCKED:-0}" == 1 ]] || ci_profile_lock shared || return
  exec 8> "${GLUTEN_DIR}/.ci/gluten-bolt-${lock_module}.lock" || return
  flock --nonblock --exclusive 8 || return
  mkdir -p "${output}" || return
  local log="${output}/run.log" status=0
  : > "${log}" || return
  export GLUTEN_TEST_ENABLE_SLOW_HIVE=false
  [[ "${stage}" != slow-hive ]] || export GLUTEN_TEST_ENABLE_SLOW_HIVE=true
  local command=("$@" -Dmaven.test.failure.ignore=false)
  local common_command=("${command[@]}")
  if [[ "${module}" == reactor && "${CI_TEST_SHARDS:-1}" != 1 ]]; then
    # Explicit suites and wildcard discovery are additive in ScalaTest. Only the
    # common invocation keeps the original wildcard; SQL uses its shard list.
    local argument filtered=()
    for argument in "${command[@]}"; do
      [[ "${argument}" == -DwildcardSuites=* ]] || filtered+=("${argument}")
    done
    command=("${filtered[@]}")
  fi
  [[ -z "${CI_SUITES:-}" ]] || command+=("-Dsuites=${CI_SUITES}")
  # Reports must come from this invocation, including when Maven fails.
  shopt -s globstar nullglob
  rm -f "${output}"/**/TEST-*.xml || return
  local reports=("${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml)
  if [[ "${module}" == reactor ]]; then
    reports=("${GLUTEN_DIR}"/**/target/*reports/TEST-*.xml)
  fi
  rm -f "${reports[@]}" || return
  cd "${GLUTEN_DIR}" || return
  if [[ "${module}" == reactor ]]; then
    # Keep the backend in the reactor so its active storage-profile dependencies
    # reach the tests. Its own tests run in JVM/Delta jobs. Run SQL separately so
    # a common-module failure cannot make Maven skip its dependent SQL tests.
    if [[ "${CI_TEST_SHARD:-1}" == 1 ]]; then
      ci_run "${log}" ci_runtime "${common_command[@]}" -Dbolt.skipTests=true \
        -pl "!gluten-ut/${SPARK_MODULE}" || status=$?
    fi
    ci_run "${log}" ci_runtime "${command[@]}" -Dbolt.skipTests=true \
      -pl "backends-bolt,gluten-ut/${SPARK_MODULE}" || status=$?
  elif [[ "${module}" == gluten-ut/* ]]; then
    ci_run "${log}" ci_runtime "${command[@]}" -Dbolt.skipTests=true \
      -pl "backends-bolt,${module}" || status=$?
  else
    ci_run "${log}" ci_runtime "${command[@]}" -pl "${module}" || status=$?
  fi
  if grep -Fq 'Cannot use org.apache.gluten.extension.GlutenSessionExtensions' "${log}"; then
    echo "Gluten session extension failed to initialize" | tee -a "${log}" >&2
    status=1
  fi
  local path suite relative executed=0 count
  reports=("${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml)
  if [[ "${module}" == reactor ]]; then
    reports=("${GLUTEN_DIR}"/**/target/*reports/TEST-*.xml)
  fi
  for path in "${reports[@]}"; do
    relative="${path#"${GLUTEN_DIR}/"}"
    mkdir -p "${output}/${relative%/*}" || return
    cp "${path}" "${output}/${relative}" || return
    # A helper-module test cannot hide an empty Spark SQL group.
    if [[ "${module}" == reactor && "${relative}" != "gluten-ut/${SPARK_MODULE}/target/"* ]]; then
      continue
    fi
    if [[ "${status}" == 0 ]]; then
      count=$(xmllint --nonet --xpath 'count(//testcase[not(skipped)])' "${path}") || return
      # Unrelated Surefire tests must not hide an empty ScalaTest selection.
      suite="${path##*/TEST-}"
      if [[ -n "${CI_SUITES:-}" && ",${CI_SUITES}," != *",${suite%.xml},"* ]]; then
        continue
      fi
      executed=$((executed + count))
    fi
  done
  if [[ "${status}" == 0 && "${executed}" == 0 ]]; then
    echo "No executed tests in fresh reports for ${stage}/${module}" | tee -a "${log}" >&2
    return 1
  fi
  return "${status}"
)
