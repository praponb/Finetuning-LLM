WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(host) AS host_lower,
        COALESCE(key, 'N/A') AS key_filled
    FROM lines
    WHERE key IS DISTINCT FROM 'trace'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        latency_ms,
        key,
        key_filled,
        ROUND(latency_ms, 1) AS latency_ms_rounded,
        FLOOR(latency_ms) AS latency_ms_whole
    FROM base
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'version'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        latency_ms * latency_ms_rounded AS latency_ms_times_latency_ms_rounded,
        CONCAT_WS('/', host, key) AS host_key_key,
        COALESCE(key, 'UNKNOWN') AS key_filled_2
    FROM filtered
    WHERE latency_ms <= 1814
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        service,
        latency_ms,
        latency_ms_whole,
        latency_ms_times_latency_ms_rounded,
        key_filled_2,
        PERCENT_RANK() OVER w_grp AS latency_ms_times_latency_ms_rounded_percent_rank,
        CUME_DIST() OVER (ORDER BY latency_ms_rounded DESC NULLS LAST) AS latency_ms_rounded_cume_dist,
        RANK() OVER (PARTITION BY key ORDER BY latency_ms NULLS LAST) AS latency_ms_rank,
        COUNT(latency_ms_whole) OVER w_running AS host_count_latency_ms_whole,
        COUNT(latency_ms_times_latency_ms_rounded) OVER (PARTITION BY key) AS key_count_latency_ms_times_latency_ms_rounded
    FROM valid
    WHERE latency_ms_whole = 32
    WINDOW w_grp AS (PARTITION BY service ORDER BY latency_ms_times_latency_ms_rounded NULLS LAST), w_running AS (PARTITION BY host)
)
SELECT
    service,
    latency_ms_times_latency_ms_rounded,
    key_filled_2,
    latency_ms_times_latency_ms_rounded_percent_rank,
    latency_ms_rank,
    key_count_latency_ms_times_latency_ms_rounded
FROM with_windows
ORDER BY latency_ms_times_latency_ms_rounded_percent_rank NULLS FIRST, latency_ms_rank DESC;
