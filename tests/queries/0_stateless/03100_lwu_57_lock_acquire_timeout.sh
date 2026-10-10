#!/usr/bin/env bash
# Tags: no-replicated-database, no-parallel, no-fasttest
# no-replicated-database - path in zookeeper differs with replicated database
# no-parallel: the `completed_pipeline_pause_before_teardown` and `patch_parts_lock_pause_before_cas`
#   failpoints are server-global, so a concurrent test would clear them while this one waits.

# A reduced check of the lightweight update lock in Keeper: `lock_acquire_timeout` and cancellation
# bound the wait in 'auto' mode, and losing the compare-and-swap on the `in_progress` directory
# retries once instead of spinning on Keeper.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# The log tables outlive the tables this test drops, and `clickhouse-test --database` reuses one
# database for every test, so tagging by database alone would let one run match another's rows.
run_id="lwu57-$CLICKHOUSE_DATABASE-$RANDOM$RANDOM"

FP=completed_pipeline_pause_before_teardown
# Only a query whose id starts with this can park at the failpoint, so unrelated pipelines cannot
# consume the one-shot arm.
QID_PREFIX=completed_pipeline_pause_failpoint_

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT patch_parts_lock_pause_before_cas" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_lwu_timeout_auto SYNC; DROP TABLE IF EXISTS t_lwu_cas SYNC" 2>/dev/null || true
}
trap cleanup EXIT

# A holder of the lightweight update lock makes a conflicting update wait for it. In 'auto' mode a
# conflict requires one update to READ the column the other WRITES
# (UpdateAffectedColumns::hasConflict), hence the first update writes `s` and the second reads it.

# Blocks until an update owns the lightweight update lock on $1. Counts the CHILDREN of a node that
# the table always has, so the count is zero until a holder takes the lock and drops back to zero
# when it releases: 'sync' takes a single `lock` node, 'auto' creates one `in_progress/update-*`
# child per update. A history-based wait would instead match an earlier holder of the same table.
function wait_for_lock_held()
{
    local table_name=$1
    local mode=$2

    local updates_path="/zookeeper/$CLICKHOUSE_DATABASE/$table_name/lightweight_updates"
    local condition="path = '$updates_path/in_progress' AND startsWith(name, 'update-')"
    if [[ "$mode" == "sync" ]]
    then
        condition="path = '$updates_path' AND name = 'lock'"
    fi

    for _ in {0..300}
    do
        sleep 0.1
        if [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.zookeeper WHERE $condition")" -gt 0 ]]
        then
            return 0
        fi
    done

    echo "Failed to wait for a $mode holder of the lightweight update lock on $table_name" >&2
    exit 2
}

# Starts an update that takes the lightweight update lock, then parks on the query thread with its
# pipeline finished but not yet torn down, holding the lock until release_holder is called. The hold
# does not begin expiring before the waiter starts, so how long a waiter blocks is chosen by this
# test rather than raced against a fixed sleep.
function start_parked_holder()
{
    local table_name=$1
    local mode=$2

    holder_qid="${QID_PREFIX}${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

    # Everything below reads the armed state as evidence, so an unarmed run would be vacuous.
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$FP'")" != 1 ]]
    then
        echo "Failed to arm the pause for a $mode holder on $table_name" >&2
        exit 2
    fi

    $CLICKHOUSE_CLIENT --query_id "$holder_qid" --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET s = 'xx' WHERE id = 2
        SETTINGS update_parallel_mode = '$mode';
    " &

    if ! $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
    then
        echo "Failed to park a $mode holder of the lightweight update lock on $table_name" >&2
        exit 2
    fi

    # The wait returns at once when nothing is parked. The failpoint is one-shot and only a query
    # whose id carries the prefix can consume it, so a zero here is this holder having parked.
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT enabled FROM system.fail_points WHERE name = '$FP'")" != 0 ]]
    then
        echo "No prefixed query parked for a $mode holder on $table_name" >&2
        exit 2
    fi

    # The lock is taken before the pipeline runs and released only when the pipeline is torn down, so
    # it must be held at the pause.
    wait_for_lock_held "$table_name" "$mode"
}

# Lets the parked holder finish and release the lock. Callers wait for the background jobs they
# started themselves, so that a waiter can be waited for separately from the holder.
function release_holder()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
}

# Server-side duration, lock try count, lost-CAS retry count and time spent acquiring the lock, for
# the query tagged with $1. The last one is the acquisition window alone, so unlike the duration it
# is not inflated by the update's own work.
function query_stats()
{
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT
            query_duration_ms,
            ProfileEvents['PatchesAcquireLockTries'],
            ProfileEvents['PatchesAcquireLockBadVersionRetries'],
            intDiv(toInt64(ProfileEvents['PatchesAcquireLockMicroseconds']), 1000)
        FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '$1' AND type != 'QueryStart'
        ORDER BY event_time_microseconds DESC LIMIT 1;
    "
}

function run_timeout()
{
    mode=auto
    table_name="t_lwu_timeout_$mode"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, s String, v UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name VALUES (1, 'aa', 0) (2, 'bb', 0) (3, 'cc', 0);
    "

    # A timeout that expires while the lock is still held must fail with TIMEOUT_EXCEEDED, and must
    # have waited close to that timeout instead of returning at once. The holder is released only
    # after this arm finishes, so the timeout is always the shorter of the two.
    timeout_ms=1000
    start_parked_holder "$table_name" "$mode"

    tag="$run_id-$mode-$timeout_ms"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 200 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = ${timeout_ms}e-3, log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    read -r duration_ms tries _ _ <<< "$(query_stats "$tag")"

    timed_out=0
    if [[ "$error" == *TIMEOUT_EXCEEDED* ]]; then timed_out=1; fi
    # The timeout is what ended the wait: it lasted about that long and no more, and it is not a
    # number of attempts each of which may itself wait the whole timeout. The upper bound is loose
    # enough for a sanitizer runner but far below the multiples an unbounded retry loop produces.
    echo "$mode $timeout_ms failed $timed_out waited $(( duration_ms >= timeout_ms * 9 / 10 && duration_ms < timeout_ms * 10 && tries <= 5 ))"

    release_holder
    wait

    # Cancellation is polled between wait chunks, so a waiter whose max_execution_time is shorter
    # than both the hold and lock_acquire_timeout must die of its own time limit rather than of the
    # lock timeout. Which error ends the wait is a fact about where the query got to, so this does
    # not read a clock; an uninterruptible wait reports the lock timeout instead.
    start_parked_holder "$table_name" "$mode"

    tag="$run_id-$mode-cancel"
    error=$($CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET v = 500 WHERE s = 'xx'
        SETTINGS update_parallel_mode = '$mode', lock_acquire_timeout = 30,
                 max_execution_time = 2, timeout_overflow_mode = 'throw', log_comment = '$tag';
    " 2>&1 >/dev/null) && error=""

    cancelled=0
    if [[ "$error" == *"Timeout exceeded:"*"maximum:"* ]]; then cancelled=1; fi
    echo "$mode cancelled-in-wait $cancelled"

    release_holder
    wait

    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

# Losing the parent-version CAS means some unrelated update committed, so there is no node to watch
# and the retry backs off a fixed amount instead of spinning on Keeper. The victim is parked between
# reading that version and using it, and a second update commits while it is parked, so the victim
# loses the compare-and-swap because the test put a commit in that window rather than because two
# concurrent writers happened to overlap in it.
function run_cas_contention()
{
    table_name="t_lwu_cas"
    tag="$run_id-cas"

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0.0;
        DROP TABLE IF EXISTS $table_name SYNC;

        CREATE TABLE $table_name (id UInt64, a UInt64, b UInt64)
        ENGINE = ReplicatedMergeTree('/zookeeper/{database}/$table_name/', '1')
        ORDER BY id
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1;

        INSERT INTO $table_name SELECT number, 0, 0 FROM numbers(5);
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT patch_parts_lock_pause_before_cas"

    $CLICKHOUSE_CLIENT --query_id "$tag" --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET a = a + 1 WHERE id = 1
        SETTINGS update_parallel_mode = 'auto', lock_acquire_timeout = 60, log_comment = '$tag';
    " &
    local victim_pid=$!

    # The wait itself is untimed, so it is bounded here: if nothing ever parks, this reports which
    # step failed instead of hanging until the whole test is killed.
    if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT patch_parts_lock_pause_before_cas PAUSE"
    then
        echo "Failed to park an update before the lightweight update lock compare-and-swap" >&2
        exit 2
    fi

    # `b` is neither read nor written by the victim, so this conflicts with nothing and only bumps
    # the version of the directory the victim is about to write. The failpoint is one-shot, so this
    # update runs straight through.
    $CLICKHOUSE_CLIENT --query "
        SET enable_lightweight_update = 1;
        UPDATE $table_name SET b = b + 1 WHERE id = 1 SETTINGS update_parallel_mode = 'auto';
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT patch_parts_lock_pause_before_cas"
    wait "$victim_pid"

    # Exactly one commit landed in the window, so the victim loses the compare-and-swap once and
    # succeeds on its next attempt. Both counts are chosen by the construction rather than by how
    # fast the runner is. PatchesAcquireLockMicroseconds encloses the parked time, so it cannot say
    # anything about the backoff here and is not asserted.
    read -r _ tries retries _ <<< "$(query_stats "$tag")"
    echo "cas retried_once $(( retries == 1 && tries == 2 ))"

    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE $table_name SYNC"
}

run_timeout
run_cas_contention
