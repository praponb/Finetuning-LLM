WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        latency_ms,
        status_code,
        attr_key,
        value,
        NTILE(3) OVER (ORDER BY latency_ms DESC, id, attr_key) AS latency_ms_bucket,
        RANK() OVER (PARTITION BY attr_key, host ORDER BY latency_ms) AS latency_ms_rank,
        SUM(status_code) OVER (PARTITION BY attr_key ORDER BY status_code, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_code
    FROM lines
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms, id, attr_key) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms_bucket NULLS LAST, id, attr_key) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(latency_ms_rank, 1) OVER (PARTITION BY attr_key ORDER BY cumulative_status_code, id, attr_key) AS next_latency_ms_rank,
        RANK() OVER (ORDER BY latency_ms_bucket) AS latency_ms_bucket_rank
    FROM latest
),
matched_keys AS (
    -- Set difference on keys
    SELECT host, id
    FROM with_windows
    EXCEPT
    SELECT host, id
    FROM app_logs
    WHERE service NOT IN ('checkout', 'auth')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.host = k.host AND s.id = k.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT host, id
    FROM reconciled
    EXCEPT
    SELECT host, id
    FROM app_logs
    WHERE latency_ms BETWEEN 1197 AND 2184
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN kept_keys k ON s.host = k.host AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        attr_key,
        host,
        latency_ms
    FROM verified
) src
PIVOT (
    MAX(latency_ms) AS amount,
    COUNT(1) AS n
    FOR host IN ('h4', 'h2')
);
