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

ci_check() {
  python3 "${BOLT_ROOT}/.github/scripts/gluten_ci.py" "$@" \
    --gluten-dir "${GLUTEN_DIR}" --reports-dir "${GLUTEN_REPORTS_DIR}"
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
  ci_check config > "${GLUTEN_REPORTS_DIR}/environment.env"
  source "${GLUTEN_REPORTS_DIR}/environment.env"
  ci_set_maven
  export SPARK_TESTING=true SPARK_SCALA_VERSION="${SCALA_VERSION}"
  unset GLUTEN_TEST_BLACKLIST
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
  ci_check spark-home --directory "${BOLT_GLUTEN_SPARK_HOME}"
  export SPARK_HOME="${BOLT_GLUTEN_SPARK_HOME}"
  local archives=("${SPARK_HOME}"/python/lib/*.zip)
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
  local log="${output}/run.log" jvm status=0
  : > "${log}" || return
  jvm="$(ci_check runner --directory "${output}/forks")" || return
  local ignore=true
  [[ "${stage}" != spark40 && "${stage}" != spark41 ]] || ignore=false
  export GLUTEN_TEST_ENABLE_SLOW_HIVE=false
  [[ "${stage}" != slow-hive ]] || export GLUTEN_TEST_ENABLE_SLOW_HIVE=true
  local command=("$@" -pl "${module}" "-Djvm=${jvm}"
    "-Dmaven.test.failure.ignore=${ignore}")
  [[ -z "${CI_SUITES:-}" ]] || command+=("-Dsuites=${CI_SUITES}")
  # Reports must come from this invocation, including when Maven fails.
  rm -f "${output}"/*TEST-*.xml || return
  local reports=("${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml)
  rm -f "${reports[@]}" || return
  cd "${GLUTEN_DIR}" || return
  ci_run "${log}" ci_runtime "${command[@]}" || status=$?
  local path
  for path in "${GLUTEN_DIR}/${module}"/target/*reports/TEST-*.xml; do
    [[ ! -f "${path}" ]] || cp "${path}" "${output}/" || return
  done
  if ((status)); then
    ci_check failed "${stage}" || true
    return "${status}"
  fi
  ci_check verify "${stage}" --group "${CI_GROUP:-}" --suites "${CI_SUITES:-}"
)

ci_environment() (
  [[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]]
  local name
  for name in bin/java bin/javac include/jni.h lib/libjawt.so; do
    test -f "${JAVA_HOME}/${name}"
  done
  "${JAVA_HOME}/bin/java" -version 2>&1 | grep -E "version \"${JDK_VERSION}[.\"]"
  python3 -c 'import sys; assert sys.version_info >= (3, 10)'
  python3 -m pip install --disable-pip-version-check \
    setuptools==77.0.3 Cython==3.3.0 numpy==1.26.4 \
    "pyspark==${SPARK_VERSION}" pandas==2.2.3 pyarrow==20.0.0
  python3 -c 'import setuptools, Cython, numpy, pyspark, pandas, pyarrow; from distutils.version import LooseVersion'
  local source="spark-${SPARK_VERSION}" directory="${BOLT_ROOT}/.ci/spark35"
  local pyspark staging digest filename extra
  pyspark="$(python3 -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).resolve().parent)')"
  mkdir -p "${directory}"
  cd "${directory}"
  local url="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}"
  curl --fail --location --retry 3 "${url}/${source}.tgz.sha512" -o "${source}.tgz.sha512"
  read -r digest filename extra < "${source}.tgz.sha512" || [[ -n "${digest}" ]]
  [[ "${digest}" =~ ^[a-fA-F0-9]{128}$ && -z "${extra}" ]]
  [[ "${filename}" == "${source}.tgz" || "${filename}" == "*${source}.tgz" ]]
  if [[ ! -f "${source}.tgz" ]]; then
    curl --fail --location --retry 3 "${url}/${source}.tgz" -o "${source}.tgz.part"
    mv "${source}.tgz.part" "${source}.tgz"
  fi
  printf '%s  %s\n' "${digest}" "${source}.tgz" | sha512sum --check -
  staging="$(mktemp -d "${directory}/spark-extract.XXXXXX")"
  trap 'rm -rf "${staging}"' EXIT
  tar --extract --gzip --no-same-owner --file "${source}.tgz" \
    --directory "${staging}" "${source}/sql"
  mkdir "${staging}/${source}-bin-hadoop3"
  for name in bin sbin jars python data examples licenses; do
    [[ ! -d "${pyspark}/${name}" ]] || ln -s "${pyspark}/${name}" "${staging}/${source}-bin-hadoop3/${name}"
  done
  ln -s "../${source}/sql" "${staging}/${source}-bin-hadoop3/sql"
  ci_check spark-home --directory "${staging}/${source}-bin-hadoop3"
  rm -rf "${directory}/${source}" "${directory}/${source}-bin-hadoop3"
  mv "${staging}/${source}" "${staging}/${source}-bin-hadoop3" "${directory}/"
  BOLT_GLUTEN_SPARK_HOME="${directory}/${source}-bin-hadoop3"
  ci_spark_environment
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    [[ "${PYTHONPATH}" != *$'\n'* && "${PYTHONPATH}" != *$'\r'* ]]
    printf 'BOLT_GLUTEN_SPARK_HOME=%s\nSPARK_HOME=%s\nPYTHONPATH=%s\nPYSPARK_PYTHON=%s\n' \
      "${BOLT_GLUTEN_SPARK_HOME}" "${SPARK_HOME}" "${PYTHONPATH}" \
      "$(command -v python3)" >> "${GITHUB_ENV}"
  fi
  printf 'Spark test home: %s\n' "${directory}/${source}-bin-hadoop3"
)
