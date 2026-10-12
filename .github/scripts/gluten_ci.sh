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
  SPARK_PROFILE="${1:-${SPARK_VERSION%.*}}"
  MVN=("${GLUTEN_DIR}/build/mvn" -ntp -Pbackends-bolt -Pdelta
    "-Pspark-${SPARK_PROFILE}" "-Pjava-${JDK_VERSION}"
    -DskipTests=false -Dmaven.test.skip=false
    -Dmaven.test.failure.ignore=false -DfailIfNoTests=false)
  if [[ "${SPARK_PROFILE}" == 4.* ]]; then
    MVN+=(-Pscala-2.13 "-Dmaven.compiler.release=${JDK_VERSION}")
  fi
}

ci_init() {
  GLUTEN_DIR="$(cd "${GLUTEN_DIR}" && pwd)"
  mkdir -p "${GLUTEN_REPORTS_DIR}"
  GLUTEN_REPORTS_DIR="$(cd "${GLUTEN_REPORTS_DIR}" && pwd)"
  SPARK_VERSION=3.5.5
  SCALA_VERSION=2.12
  JDK_VERSION=17
  ci_set_maven
  export SPARK_TESTING=true SPARK_SCALA_VERSION="${SCALA_VERSION}"
  export GLUTEN_TEST_BLACKLIST="${GLUTEN_TEST_BLACKLIST:-${BOLT_ROOT}/.github/gluten-ci-blacklist.txt}"
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
  BOLT_GLUTEN_SPARK_HOME="${BOLT_GLUTEN_SPARK_HOME:-/opt/shims/spark35/spark_home}"
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
  if [[ "${stage}" == spark40 || "${stage}" == spark41 ]]; then
    unset GLUTEN_TEST_BLACKLIST
  fi
  export GLUTEN_TEST_ENABLE_SLOW_HIVE=false
  [[ "${stage}" != slow-hive ]] || export GLUTEN_TEST_ENABLE_SLOW_HIVE=true
  local command=("$@" -pl "${module}" -Dmaven.test.failure.ignore=false)
  [[ -z "${CI_SUITES:-}" ]] || command+=("-Dsuites=${CI_SUITES}")
  # Reports must come from this invocation, including when Maven fails.
  rm -f "${output}"/*TEST-*.xml || return
  local reports=("${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml)
  rm -f "${reports[@]}" || return
  cd "${GLUTEN_DIR}" || return
  ci_run "${log}" ci_runtime "${command[@]}" || status=$?
  local path suite executed=0 count
  for path in "${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml; do
    [[ -f "${path}" ]] || continue
    cp "${path}" "${output}/" || return
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
