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

"""Check or refresh the byte-for-byte mirror of Gluten's reusable Bolt workflow."""

import argparse
import hashlib
import subprocess
from pathlib import Path

CANONICAL_REPOSITORY = "Weixin-Xu/gluten"
CANONICAL_COMMIT = "2e8571283d6632b3cd2f05d5cfc43acdd6d4199c"
CANONICAL_SHA256 = "b6bececd87e31cd5fc437cab5a919a0f1e9fa702202123b8ebb7195ff01f05b9"
WORKFLOW = ".github/workflows/bolt_backend_x86.yml"
MIRROR = Path(__file__).resolve().parents[2] / WORKFLOW


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gluten-repo",
        type=Path,
        help=f"Local checkout containing {CANONICAL_REPOSITORY}@{CANONICAL_COMMIT}",
    )
    parser.add_argument(
        "--write", action="store_true", help="Replace the mirror with the pinned bytes"
    )
    args = parser.parse_args()
    if args.write and args.gluten_repo is None:
        parser.error("--write requires --gluten-repo")
    if args.gluten_repo is None:
        canonical = MIRROR.read_bytes()
    else:
        canonical = subprocess.check_output(
            ["git", "-C", args.gluten_repo, "show", f"{CANONICAL_COMMIT}:{WORKFLOW}"]
        )
    if hashlib.sha256(canonical).hexdigest() != CANONICAL_SHA256:
        parser.exit(
            1,
            f"Workflow digest differs from {CANONICAL_REPOSITORY}@{CANONICAL_COMMIT}\n",
        )
    if args.write:
        MIRROR.write_bytes(canonical)
    elif MIRROR.read_bytes() != canonical:
        parser.exit(1, "The mirror differs from the pinned Gluten workflow\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
