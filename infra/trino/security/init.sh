#!/usr/bin/env bash
# TLS keystore and password file of Trino's HTTPS listener (ADR 0011).
#
# Runs on every `up` through the trino-init service. The key pair is kept while the keystore
# opens with the configured password, so clients that pinned the certificate keep working; the
# password file is rewritten from the environment, so a changed password applies on restart.
set -euo pipefail

dir=/etc/trino/security
keystore=$dir/keystore.p12

if ! keytool -list -keystore "$keystore" -storetype PKCS12 -storepass "$TRINO_KEYSTORE_PASSWORD" >/dev/null 2>&1; then
  rm -f "$keystore" "$dir/trino.crt"
  # localhost for clients on the host (Power BI Desktop), trino for those in the compose network.
  keytool -genkeypair -alias trino -keyalg RSA -keysize 3072 -validity 825 \
    -dname "CN=localhost, O=AML compliance platform" -ext "SAN=dns:localhost,dns:trino,ip:127.0.0.1" \
    -keystore "$keystore" -storetype PKCS12 -storepass "$TRINO_KEYSTORE_PASSWORD"
  keytool -exportcert -rfc -alias trino -keystore "$keystore" -storetype PKCS12 \
    -storepass "$TRINO_KEYSTORE_PASSWORD" -file "$dir/trino.crt"
fi

# Only people log in with a password; services stay on the network-internal HTTP listener.
java /init/HashPassword.java $TRINO_PASSWORD_USERS >"$dir/password.db.new"
mv "$dir/password.db.new" "$dir/password.db"

chown trino:trino "$keystore" "$dir/password.db" "$dir/trino.crt"
chmod 0400 "$keystore" "$dir/password.db"
chmod 0444 "$dir/trino.crt"
