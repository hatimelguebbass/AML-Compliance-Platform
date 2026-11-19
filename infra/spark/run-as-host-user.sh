#!/usr/bin/env bash
# Compose runs this container with the host user's UID so that files written under
# data/ stay owned by that user. That UID has no passwd entry in the image, and
# Hadoop's login (used by spark-submit) needs a user name and a home directory:
# nss_wrapper provides both without making /etc/passwd writable.
set -euo pipefail

if ! getent passwd "$(id -u)" >/dev/null; then
  wrapper=$(find /usr/lib -name libnss_wrapper.so -print -quit)
  NSS_WRAPPER_PASSWD=$(mktemp)
  NSS_WRAPPER_GROUP=$(mktemp)
  printf 'spark:x:%s:%s:spark:/tmp:/bin/false\n' "$(id -u)" "$(id -g)" >"$NSS_WRAPPER_PASSWD"
  printf 'spark:x:%s:\n' "$(id -g)" >"$NSS_WRAPPER_GROUP"
  export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
fi

exec "$@"
