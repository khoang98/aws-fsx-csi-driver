#!/bin/bash

# Copyright 2026 The Kubernetes Authors.
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

# Cleans up FSx Lustre filesystems older than a specified age.
# This prevents stranded filesystems from e2e test failures from accumulating
# and exhausting the account's FSx storage quota.

set -euo pipefail

REGION=${AWS_REGION:-us-west-2}
MAX_AGE_DAYS=${MAX_AGE_DAYS:-7}

echo "Cleaning up FSx Lustre filesystems older than ${MAX_AGE_DAYS} days in ${REGION}"

CUTOFF_EPOCH=$(( $(date +%s) - (MAX_AGE_DAYS * 86400) ))

# Get all Lustre filesystem IDs and creation times
FS_LIST=$(aws fsx describe-file-systems \
  --region "${REGION}" \
  --output json \
  --query 'FileSystems[?FileSystemType==`LUSTRE`].{Id:FileSystemId,Created:CreationTime,Capacity:StorageCapacity,Lifecycle:Lifecycle}')

if [[ -z "${FS_LIST}" || "${FS_LIST}" == "[]" || "${FS_LIST}" == "null" ]]; then
  echo "No Lustre filesystems found"
  exit 0
fi

TOTAL=$(echo "${FS_LIST}" | jq length)
echo "Found ${TOTAL} Lustre filesystem(s)"

DELETED=0
SKIPPED=0

for row in $(echo "${FS_LIST}" | jq -r '.[] | @base64'); do
  _jq() {
    echo "${row}" | base64 --decode | jq -r "${1}"
  }

  FS_ID=$(_jq '.Id')
  CREATION_TIME=$(_jq '.Created')
  CAPACITY=$(_jq '.Capacity')
  LIFECYCLE=$(_jq '.Lifecycle')

  # FSx API returns creation time as Unix epoch float (e.g., 1754330438.291)
  # Truncate to integer for comparison
  FS_EPOCH=${CREATION_TIME%.*}

  if [[ "${FS_EPOCH}" -lt "${CUTOFF_EPOCH}" ]]; then
    AGE_DAYS=$(( ($(date +%s) - FS_EPOCH) / 86400 ))
    echo "  DELETE ${FS_ID}: ${AGE_DAYS} days old, ${CAPACITY} GiB, lifecycle=${LIFECYCLE}"

    if [[ "${LIFECYCLE}" == "DELETING" ]]; then
      echo "    Already deleting, skipping"
      SKIPPED=$((SKIPPED + 1))
      continue
    fi

    aws fsx delete-file-system \
      --region "${REGION}" \
      --file-system-id "${FS_ID}" \
      --output text > /dev/null 2>&1 || echo "    WARNING: failed to delete ${FS_ID}"

    DELETED=$((DELETED + 1))
  else
    SKIPPED=$((SKIPPED + 1))
  fi
done

echo "Cleanup complete. Deleted: ${DELETED}, Skipped: ${SKIPPED}"
