DROP TABLE IF EXISTS wikistat1;
DROP TABLE IF EXISTS wikistat2;

SET apply_mutations_on_fly = 0;

CREATE TABLE wikistat1
(
    time DateTime,
    project LowCardinality(String),
    subproject LowCardinality(String),
    path String,
    hits UInt64,
    PROJECTION total
    (
        SELECT
            project,
            subproject,
            path,
            sum(hits),
            count()
        GROUP BY
            project,
            subproject,
            path
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02494_zero_copy_and_projection', '1')
ORDER BY (path, time)
SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration=0, allow_remote_fs_zero_copy_replication=1, min_bytes_for_wide_part=0;

CREATE TABLE wikistat2
(
    time DateTime,
    project LowCardinality(String),
    subproject LowCardinality(String),
    path String,
    hits UInt64,
    PROJECTION total
    (
        SELECT
            project,
            subproject,
            path,
            sum(hits),
            count()
        GROUP BY
            project,
            subproject,
            path
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02494_zero_copy_and_projection', '2')
ORDER BY (path, time)
SETTINGS old_parts_lifetime = 1, cleanup_delay_period = 0, cleanup_delay_period_random_add = 0,
    cleanup_thread_preferred_points_per_iteration=0, allow_remote_fs_zero_copy_replication=1, min_bytes_for_wide_part=0;

INSERT INTO wikistat1 SELECT toDateTime('2020-10-01 00:00:00'), 'hello', 'world', '/data/path', 10 from numbers(100);

INSERT INTO wikistat1 SELECT toDateTime('2020-10-01 00:00:00'), 'hello', 'world', '/data/path', 10 from numbers(99, 99);

SYSTEM SYNC REPLICA wikistat2;

SELECT COUNT() from wikistat1 WHERE NOT ignore(*);
SELECT COUNT() from wikistat2 WHERE NOT ignore(*);

SYSTEM STOP REPLICATION QUEUES wikistat2;

ALTER TABLE wikistat1 DELETE where time = toDateTime('2022-12-20 00:00:00') SETTINGS mutations_sync = 1;

SYSTEM START REPLICATION QUEUES wikistat2;

SYSTEM SYNC REPLICA wikistat2;

-- it doesn't make test flaky, rarely we will not delete the parts because of cleanup thread was slow.
-- Such condition will lead to successful queries.
SET function_sleep_max_microseconds_per_block = 5000000;
SELECT 0 FROM numbers(5) WHERE sleepEachRow(1) = 1;

select sum(hits), count() from wikistat1 GROUP BY project, subproject, path settings optimize_use_projections = 1, force_optimize_projection = 1;
select sum(hits), count() from wikistat2 GROUP BY project, subproject, path settings optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE wikistat1;
DROP TABLE wikistat2;
