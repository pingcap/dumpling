#!/bin/sh

set -eu

export TEST_DIR=/tmp/dumpling_test_result
export TEST_USER=root
export TEST_HOST=127.0.0.1
export TEST_PORT=3306
export TEST_PASSWORD=""

mkdir -p "$TEST_DIR"
PATH="tests/_utils:$PATH"

test_connection() {
  i=0
  while ! run_sql 'select 0 limit 0' > /dev/null; do
      i=$((i+1))
      if [ "$i" -gt 10 ]; then
          echo 'Failed to ping MySQL Server'
          exit 1
      fi
      sleep 3
  done
}

test_connection

for script in tests/*/run.sh; do
    echo "Running test $script..."
    BASE_NAME="$(dirname "$script")"
    export BASE_NAME
    TEST_NAME="$(basename "$(dirname "$script")")"
    OUTPUT_DIR="$TEST_DIR"/sql_res."$TEST_NAME"
    export OUTPUT_DIR

    PATH="tests/_utils:$PATH" \
    sh "$script"
done

echo "Passed integration tests."
