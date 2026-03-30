#!/bin/sh
set -e

# Find iscsid PID on host
PID=$(pgrep iscsid | head -n 1)

if [ -z "$PID" ]; then
    echo "Error: iscsid not running on host" >&2
    exit 1
fi

# Find iscsiadm path on host
HOST_ISCSADM=""
for path in /usr/local/sbin/iscsiadm /usr/sbin/iscsiadm /sbin/iscsiadm /usr/bin/iscsiadm /bin/iscsiadm; do
    if ls "/proc/$PID/root$path" >/dev/null 2>&1; then
        HOST_ISCSADM="$path"
        break
    fi
done

if [ -z "$HOST_ISCSADM" ]; then
    echo "Error: iscsiadm not found on host" >&2
    exit 1
fi

# Execute on host using nsenter
# We share the mount/network/ipc/uts namespaces of the iscsid process
exec nsenter -t "$PID" -m -n -i -u -- "$HOST_ISCSADM" "$@"
