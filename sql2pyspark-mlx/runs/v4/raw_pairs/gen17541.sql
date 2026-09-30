WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS k, v
),
unpacked AS (
    SELECT
        service,
        level,
        k,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM flattened
    WHERE k = 'trace'
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY k, service ORDER BY status_code_high) AS status_code_high_cume_dist,
        RANK() OVER (PARTITION BY k ORDER BY is_flagged DESC NULLS FIRST) AS is_flagged_rank,
        PERCENT_RANK() OVER (PARTITION BY k, level ORDER BY is_flagged NULLS LAST) AS is_flagged_percent_rank
    FROM unpacked
),
with_windows_app_logs AS (
    SELECT
        *,
        MAX(is_flagged_percent_rank) OVER (PARTITION BY level) AS level_max_is_flagged_percent_rank,
        RANK() OVER (PARTITION BY level ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
        STDDEV_SAMP(is_flagged_rank) OVER (PARTITION BY k) AS k_stddev_samp_is_flagged_rank
    FROM with_windows
),
valid AS (
    SELECT
        service,
        k,
        is_flagged,
        status_code_high,
        level_max_is_flagged_percent_rank,
        k_stddev_samp_is_flagged_rank,
        k_stddev_samp_is_flagged_rank / NULLIF(status_code_high_cume_dist, 0) AS k_stddev_samp_is_flagged_rank_per_status_code_high_cume_dist
    FROM with_windows_app_logs
    WHERE k IS NOT NULL
),
filtered AS (
    SELECT
        k,
        is_flagged,
        k_stddev_samp_is_flagged_rank,
        INITCAP(TRIM(service)) AS service_title
    FROM valid
    WHERE k IN ('version', 'region', 'trace')
),
with_windows_app_logs_2 AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY k_stddev_samp_is_flagged_rank NULLS FIRST) AS k_stddev_samp_is_flagged_rank_percent_rank,
        ROUND(k_stddev_samp_is_flagged_rank / SUM(k_stddev_samp_is_flagged_rank) OVER (PARTITION BY k), 4) AS k_stddev_samp_is_flagged_rank_share
    FROM filtered
)
SELECT
    is_flagged,
    service_title,
    k_stddev_samp_is_flagged_rank_share
FROM with_windows_app_logs_2;
