WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS attr_key, value
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, HOUR(log_ts) AS log_ts_hour
    FROM exploded
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'version'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY level), 4) AS status_code_share,
        ROW_NUMBER() OVER (PARTITION BY host ORDER BY status_code DESC, id ASC, attr_key ASC) AS event_seq,
        COUNT(log_ts_hour) OVER (PARTITION BY level ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS log_ts_hour_count_15m,
        PERCENT_RANK() OVER (ORDER BY log_ts_hour DESC NULLS FIRST) AS log_ts_hour_percent_rank
    FROM valid
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms ASC, id ASC, attr_key ASC) AS rn
    FROM enriched
    QUALIFY rn = 1
)
-- Window metrics over several different partitions
SELECT
    id,
    host,
    log_ts,
    status_code,
    attr_key,
    event_seq,
    log_ts_hour_percent_rank,
    SUM(latency_ms) OVER w_ordered AS running_latency_ms,
    MAX(log_ts_hour_percent_rank) OVER (PARTITION BY host, attr_key) AS host_max_log_ts_hour_percent_rank,
    AVG(log_ts_hour_count_15m) OVER (PARTITION BY attr_key ORDER BY log_ts_hour_count_15m ASC, id ASC, attr_key ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS log_ts_hour_count_15m_avg_3_prior
FROM deduped
WINDOW w_ordered AS (PARTITION BY host ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
