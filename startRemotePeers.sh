#!/usr/bin/env bash
# Launches peer processes on remote hosts in the order listed in PeerInfo.cfg.
# Usage: ./startRemotePeers.sh [remote-working-directory]
# Default remote dir: ~/project
#
# Assumes:
#   - Passwordless SSH to each hostname in PeerInfo.cfg.
#   - The working directory on each remote host contains compiled .class files,
#     Common.cfg, PeerInfo.cfg, and (for seed peers) the data file placed in
#     peer_<id>/.

set -u

REMOTE_DIR="${1:-~/project}"
PEERINFO="PeerInfo.cfg"

if [ ! -f "$PEERINFO" ]; then
    echo "Error: $PEERINFO not found in current directory" >&2
    exit 1
fi

while read -r PEER_ID HOST PORT HAS_FILE _rest; do
    [ -z "${PEER_ID:-}" ] && continue
    echo "Launching Peer $PEER_ID on $HOST..."
    ssh -n "$HOST" "cd $REMOTE_DIR && nohup java PeerProcess $PEER_ID >peer_${PEER_ID}.out 2>&1 &"
    sleep 1
done < "$PEERINFO"

echo "All peers launched. Check log_peer_*.log on each host."
