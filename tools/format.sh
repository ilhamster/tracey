#!/usr/bin/env bash
set -euo pipefail

buildifier="${BUILDIFIER:-}"
gofmt_bin="${GOFMT:-}"
buildifier_runfile=""
gofmt_runfile=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --buildifier)
      buildifier_runfile="$2"
      shift 2
      ;;
    --gofmt)
      gofmt_runfile="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

resolve_runfile() {
  local path="$1"
  if [[ -n "${RUNFILES_DIR:-}" ]]; then
    echo "${RUNFILES_DIR}/${path}"
    return 0
  fi
  if [[ -n "${RUNFILES_MANIFEST_FILE:-}" ]]; then
    local entry
    entry="$(grep -m1 "^${path} " "${RUNFILES_MANIFEST_FILE}" || true)"
    if [[ -n "${entry}" ]]; then
      echo "${entry#* }"
      return 0
    fi
  fi
  local script_runfiles="${0}.runfiles"
  if [[ -d "${script_runfiles}" ]]; then
    echo "${script_runfiles}/${path}"
    return 0
  fi
  local script_manifest="${0}.runfiles_manifest"
  if [[ -f "${script_manifest}" ]]; then
    local entry
    entry="$(grep -m1 "^${path} " "${script_manifest}" || true)"
    if [[ -n "${entry}" ]]; then
      echo "${entry#* }"
      return 0
    fi
  fi
  return 1
}

if [[ -z "${buildifier}" ]]; then
  if [[ -n "${buildifier_runfile}" ]]; then
    if [[ -x "${buildifier_runfile}" ]]; then
      buildifier="${buildifier_runfile}"
    elif [[ -x "$(dirname "$0")/${buildifier_runfile}" ]]; then
      buildifier="$(dirname "$0")/${buildifier_runfile}"
    else
      buildifier="$(resolve_runfile "${buildifier_runfile}" || true)"
      buildifier="${buildifier:-${buildifier_runfile}}"
    fi
  else
    buildifier="$(resolve_runfile buildifier_prebuilt/buildifier || true)"
  fi
fi
buildifier="${buildifier:-buildifier}"

if [[ -z "${gofmt_bin}" ]]; then
  if [[ -n "${gofmt_runfile}" ]]; then
    if [[ -x "${gofmt_runfile}" ]]; then
      gofmt_bin="${gofmt_runfile}"
    elif [[ -x "$(dirname "$0")/${gofmt_runfile}" ]]; then
      gofmt_bin="$(dirname "$0")/${gofmt_runfile}"
    else
      gofmt_bin="$(resolve_runfile "${gofmt_runfile}" || true)"
      gofmt_bin="${gofmt_bin:-${gofmt_runfile}}"
    fi
  else
    gofmt_bin="$(resolve_runfile go_sdk/bin/gofmt || true)"
  fi
fi
gofmt_bin="${gofmt_bin:-gofmt}"

root="${BUILD_WORKSPACE_DIRECTORY:-$(pwd)}"

if command -v rg >/dev/null 2>&1; then
  "${buildifier}" -lint=fix $(rg --files -g 'BUILD' -g 'BUILD.bazel' -g '*.bzl' "${root}")
  "${gofmt_bin}" -w $(rg --files -g '*.go' "${root}")
else
  "${buildifier}" -lint=fix $(find "${root}" -type f \( -name 'BUILD' -o -name 'BUILD.bazel' -o -name '*.bzl' \))
  "${gofmt_bin}" -w $(find "${root}" -type f -name '*.go')
fi
