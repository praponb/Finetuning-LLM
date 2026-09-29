WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, attr_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN attr_key IN ('trace', 'version') THEN 'Core' ELSE 'Other' END attr_key_group,
        COALESCE(attr_key, 'DIRECT') attr_key_filled
    FROM flattened
    WHERE latency_ms > 1857
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        attr_key,
        attr_key_group,
        attr_key_filled,
        ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY latency_ms DESC, id, attr_key) event_seq,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY attr_key) attr_key_stddev_samp_latency_ms,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER (PARTITION BY host ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_latency_ms,
        LAST_VALUE(latency_ms, TRUE) OVER (PARTITION BY service ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_latency_ms_2
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY level, attr_key ORDER BY event_seq, id, attr_key) seq_no,
        NTILE(3) OVER (ORDER BY event_seq, id, attr_key) event_seq_bucket,
        MIN(latency_ms) OVER (PARTITION BY host) host_min_latency_ms,
        NTILE(10) OVER (PARTITION BY service, attr_key ORDER BY last_known_latency_ms, id, attr_key) last_known_latency_ms_bucket,
        ROW_NUMBER() OVER (ORDER BY latency_ms DESC, id, attr_key) row_seq
    FROM scored
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service, level ORDER BY attr_key_stddev_samp_latency_ms DESC, id, attr_key) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY last_known_latency_ms_2 DESC, id, attr_key) AS rank_in_group
        FROM latest
    ) x
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last_known_latency_ms_bucket) OVER (ORDER BY host_min_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last_known_latency_ms_bucket,
        ROW_NUMBER() OVER (ORDER BY latency_ms DESC, id, attr_key) seq_no_2,
        NTILE(10) OVER (PARTITION BY service ORDER BY attr_key_stddev_samp_latency_ms DESC, id, attr_key) attr_key_stddev_samp_latency_ms_bucket,
        MIN(latency_ms) OVER (PARTITION BY service) service_min_latency_ms
    FROM ranked
)
SELECT
    id,
    host,
    level,
    latency_ms,
    attr_key,
    attr_key_filled,
    last_known_latency_ms,
    seq_no,
    host_min_latency_ms,
    CASE WHEN level RLIKE '^(WARN|ERROR)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS level_zone
FROM with_windows
ORDER BY level DESC;
