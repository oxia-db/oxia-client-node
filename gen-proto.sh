#!/usr/bin/env bash
# Generates TypeScript code from proto/*.proto into src/proto/generated.
# Requires `npm install` to have been run (ts-proto is a dev dependency).
set -euo pipefail

cd "$(dirname "$0")"

OUT_DIR="src/proto/generated"
PROTO_DIR="proto"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

# ts-proto options:
#   outputServices=grpc-js      -> generate @grpc/grpc-js client stubs
#   esModuleInterop=true        -> play nice with tsconfig esModuleInterop
#   useOptionals=messages       -> proto3 `optional` -> TS `?:` field
#   oneof=unions                -> tagged union for `oneof`
#   forceLong=long              -> int64 as Long (no precision loss)
#   useExactTypes=false         -> don't require discriminator properties
npx --no-install protoc \
  --plugin=protoc-gen-ts_proto="./node_modules/.bin/protoc-gen-ts_proto" \
  --ts_proto_out="$OUT_DIR" \
  --ts_proto_opt=outputServices=grpc-js,esModuleInterop=true,useOptionals=messages,oneof=unions,forceLong=long,useExactTypes=false \
  --proto_path="$PROTO_DIR" \
  "$PROTO_DIR"/*.proto

echo "Generated $(find "$OUT_DIR" -name '*.ts' | wc -l | tr -d ' ') file(s) in $OUT_DIR"
