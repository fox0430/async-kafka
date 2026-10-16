#!/bin/sh
# Generates the throwaway CA and server certificate for the integration
# broker's SASL_SSL listener. Run by the `certgen` service in
# docker-compose.yml; the output directory is a bind mount (docker/certs) so
# the Nim tests can read the CA, and it is git-ignored: the key is a local
# artifact, not repository content.
set -eu

out=/certs/out
mkdir -p "$out"

# Reuse an existing set. Regenerating the CA while a broker is still running (a
# second `docker compose up` that does not recreate the broker) would leave
# docker/certs/ca.crt describing a CA the running keystore does not chain to,
# and every TLS test would then fail the handshake with nothing pointing at the
# files. Delete the directory to force a fresh certificate.
if [ -f "$out/ca.crt" ] && [ -f "$out/server.p12" ] &&
  [ -f "$out/key_password" ] && [ -f "$out/keystore_password" ]; then
  echo "reusing the certificate already in $out"
  exit 0
fi

# CA
openssl genrsa -out "$out/ca.key" 2048 2>/dev/null
openssl req -x509 -new -sha256 -days 3650 \
  -key "$out/ca.key" -out "$out/ca.crt" \
  -subj "/CN=async-kafka integration test CA" \
  -addext "basicConstraints=critical,CA:TRUE" 2>/dev/null

# Server certificate. The client dials and verifies `localhost` (the name the
# listener advertises), so that has to be a dNSName SAN: BearSSL matches names
# against SANs, not against the CN, and not against IP SANs. The IP SAN is kept
# for tooling that dials the literal.
openssl genrsa -out "$out/server.key" 2048 2>/dev/null
openssl req -new -sha256 \
  -key "$out/server.key" -out "$out/server.csr" \
  -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -sha256 -days 3650 \
  -in "$out/server.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" -CAcreateserial \
  -extfile /dev/stdin -out "$out/server.crt" <<'EOF' 2>/dev/null
subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
EOF

# The JVM reads the listener's key material from a keystore, not from PEM.
openssl pkcs12 -export \
  -inkey "$out/server.key" -in "$out/server.crt" -certfile "$out/ca.crt" \
  -name kafka -out "$out/server.p12" -passout pass:changeit

# The Kafka image takes the keystore path and both passwords from files next to
# the keystore (`KAFKA_SSL_KEYSTORE_FILENAME` and the *_CREDENTIALS variables),
# so those files are part of what this service has to produce.
printf 'changeit' >"$out/key_password"
printf 'changeit' >"$out/keystore_password"

rm -f "$out/server.csr" "$out/ca.srl"

# Root-owned files: keep the CA readable by the test process, the keys private.
chmod 644 "$out"/*.crt "$out"/server.p12 "$out"/*password
chmod 600 "$out"/*.key
