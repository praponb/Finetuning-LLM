WITH flattened AS (
    SELECT
        id,
        host,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
cleaned AS (
    SELECT
        *,
        COALESCE(key, 'Unknown') AS key_filled,
        COALESCE(latency_ms, 0) AS latency_ms_filled,
        COALESCE(key, 'UNKNOWN') AS key_filled_2
    FROM flattened
    WHERE key IS NOT DISTINCT FROM 'trace'
      AND key IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        host,
        RANK() OVER (PARTITION BY host ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY host, key), 4) AS latency_ms_share
    FROM cleaned
    WHERE key <> 'version'
),
filtered AS (
    SELECT *, TRIM(host) AS clean_host
    FROM with_windows
    WHERE latency_ms_share <= 0.32
),
cleaned_app_logs AS (
    SELECT *, ROUND(latency_ms_share, 2) AS latency_ms_share_rounded
    FROM filtered
    WHERE host IN ('h3', 'h1')
),
scored AS (
    SELECT
        latency_ms_share,
        clean_host,
        RANK() OVER (PARTITION BY host ORDER BY latency_ms_share_rounded DESC NULLS FIRST) AS latency_ms_share_rounded_rank
    FROM cleaned_app_logs
)
SELECT latency_ms_share, latency_ms_share_rounded_rank
FROM scored
