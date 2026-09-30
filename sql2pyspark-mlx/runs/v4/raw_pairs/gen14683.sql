WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, v
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        latency_ms,
        status_code,
        attr_key,
        v,
        CORR(latency_ms, status_code) OVER (PARTITION BY attr_key) AS corr_latency_ms_status_code,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share
    FROM lines
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(latency_ms_share) OVER (PARTITION BY attr_key) AS attr_key_avg_latency_ms_share,
        CORR(status_code, latency_ms) OVER (PARTITION BY attr_key) AS corr_status_code_latency_ms,
        RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms) AS latency_ms_rank
    FROM enriched
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(attr_key) AS attr_key_lower
    FROM scored
    WHERE attr_key IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms_share DESC) AS latency_ms_share_percent_rank,
        SUM(latency_ms_share) OVER (PARTITION BY attr_key) AS attr_key_sum_latency_ms_share,
        MAX(attr_key_avg_latency_ms_share) OVER (PARTITION BY attr_key) AS attr_key_max_attr_key_avg_latency_ms_share,
        PERCENT_RANK() OVER (ORDER BY latency_ms_share DESC NULLS FIRST) AS latency_ms_share_percent_rank_2,
        AVG(latency_ms_rank) OVER (PARTITION BY attr_key) AS attr_key_avg_latency_ms_rank
    FROM cleaned
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM app_logs
    WHERE level = 'WARN'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    latency_ms,
    status_code,
    attr_key,
    corr_status_code_latency_ms,
    latency_ms_rank,
    latency_ms_share_percent_rank,
    attr_key_sum_latency_ms_share,
    latency_ms_share_percent_rank_2,
    attr_key_avg_latency_ms_rank
FROM reconciled
ORDER BY status_code
