#!/bin/sh

set -e

SOURCE_FILE=".env"
TARGET_FILE=".env.sample"

cp "$SOURCE_FILE" "$TARGET_FILE"

sed -i.bak 's/=.*/=/' "$TARGET_FILE" && rm -f "$TARGET_FILE.bak"
sed -i.bak '1s/^/# Generated automatically by pre-commit\n/' "$TARGET_FILE" && rm -f "$TARGET_FILE.bak"

git add "$TARGET_FILE"
