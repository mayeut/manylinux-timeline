#!/bin/bash

# Prevents script from running if there are any errors
set -euo pipefail

NOCHECK=0
NOCOMMIT=0
NOPUSH=0

echo_usage() {
	echo "Usage: $0 [-h|--help] [--no-check] [--no-commit] [--no-push]"
	echo "  -h, --help:  show this help message and exits"
	echo "  --no-check:  do not check working tree is clean"
	echo "  --no-commit: do not commit the updated cache. Implies --no-push"
	echo "  --no-push:   do not push the updated cache"
}

while [[ $# -gt 0 ]]; do
	key="$1"

	case ${key} in
	--no-check) NOCHECK=1; shift;;
	--no-commit) NOCOMMIT=1; shift;;
	--no-push) NOPUSH=1; shift;;
	-h|--help) echo_usage; exit 0;;
	*) echo "ERROR: Unkown option\"${key}\"" 1>&2; echo_usage 1>&2; exit 1;;
	esac
done

# Show commands
set -x

# Info
python3 --version

# Install dependencies
python3 -m pip install -r requirements.txt

# Update main
git checkout --quiet main > /dev/null
git pull --quiet --ff-only origin main > /dev/null

# Check for local modifications
if [[ ${NOCHECK} -eq 0 ]]; then
	test -z "$(git status --porcelain)"
fi

# Generate the files
python3 update.py -v

if ! git diff --quiet --exit-code; then
	if [[ ${NOCOMMIT} -ne 0 ]]; then
		echo "Skipping commit"
		exit 0
	fi
	TIMESTAMP=$(python3 -c 'from datetime import datetime,timezone; print(datetime.now(timezone.utc).strftime("%A, %d %B %Y, %H:%M:%S %Z"))')
	git add -u
	git commit -m "Update cache ${TIMESTAMP}"
	if [[ ${NOPUSH} -ne 0 ]]; then
		echo "Skipping push"
		exit 0
	fi
	git push
fi
