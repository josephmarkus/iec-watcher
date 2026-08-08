#!/bin/bash
# One-time setup for the IEC watcher LaunchAgent.
#
# This script only touches user-level state (no sudo). The one privileged
# step — scheduling a hardware wake so the Mac Mini is awake at check time —
# is deliberately NOT run automatically here. It's printed at the end for
# you to run yourself once you're happy with everything.
set -euo pipefail

PROJECT_DIR="/Users/josephmarkus/Projects/iec-watcher"
PLIST_NAME="com.josephmarkus.iecwatcher.plist"
SRC_PLIST="${PROJECT_DIR}/${PLIST_NAME}"
DEST_PLIST="${HOME}/Library/LaunchAgents/${PLIST_NAME}"
UID_NUM="$(id -u)"

echo "== IEC watcher setup =="

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${SRC_PLIST}" "${DEST_PLIST}"
echo "Installed plist -> ${DEST_PLIST}"

# Unload first in case this is a re-install.
launchctl bootout "gui/${UID_NUM}" "${DEST_PLIST}" >/dev/null 2>&1 || true

launchctl bootstrap "gui/${UID_NUM}" "${DEST_PLIST}"
echo "Loaded LaunchAgent com.josephmarkus.iecwatcher (fires daily at 16:05 Europe/London)."

echo
echo "== ntfy.sh =="
TOPIC="$(python3 -c "import json; print(json.load(open('${PROJECT_DIR}/config.json'))['ntfy_topic'])")"
echo "Topic: ${TOPIC}"
echo "Subscribe on your iPhone: open the ntfy app -> Subscribe to topic -> enter '${TOPIC}'"
echo "(Or scan/visit: https://ntfy.sh/${TOPIC})"

echo
echo "== One remaining manual step =="
echo "For the Mac to actually be AWAKE at 16:05 even if it's asleep, schedule a"
echo "recurring hardware wake. This needs sudo and only needs to be run ONCE"
echo "(it persists across reboots). Run it yourself when ready:"
echo
echo "    sudo pmset repeat wake MTWRFSU 16:00:00"
echo
echo "Verify afterwards with: pmset -g sched"
