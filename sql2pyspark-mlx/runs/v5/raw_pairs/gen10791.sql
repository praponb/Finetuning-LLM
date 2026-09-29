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
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, AVG(latency_ms) OVER (PARTITION BY attr_key) AS attr_key_avg_latency_ms
    FROM lines
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, latency_ms + status_code AS latency_ms_plus_status_code
    FROM with_windows
    WHERE host = 'h3'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        attr_key,
        value,
        latency_ms_plus_status_code,
        CORR(latency_ms, status_code) OVER (PARTITION BY attr_key) AS corr_latency_ms_status_code,
        RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_rank
    FROM cleaned
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        corr_latency_ms_status_code * latency_ms_plus_status_code AS corr_latency_ms_status_code_times_latency_ms_plus_status_code,
        ROUND(latency_ms_plus_status_code, 0) AS latency_ms_plus_status_code_rounded,
        COALESCE(attr_key, 'DIRECT') AS attr_key_clean
    FROM metrics
    WHERE attr_key IS NOT NULL
      AND corr_latency_ms_status_code BETWEEN -0.5 AND -0.3
)
SELECT
    id,
    attr_key,
    latency_ms_plus_status_code,
    corr_latency_ms_status_code,
    latency_ms_rank,
    corr_latency_ms_status_code_times_latency_ms_plus_status_code
FROM valid
ORDER BY attr_key DESC NULLS LAST;
