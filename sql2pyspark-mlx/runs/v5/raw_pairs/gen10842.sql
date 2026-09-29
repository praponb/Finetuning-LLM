WITH exploded AS (
    SELECT
        id,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
unpacked AS (
    SELECT
        id,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM exploded
),
valid AS (
    SELECT
        *,
        COALESCE(k, 'unassigned') AS k_filled,
        status_code_high / NULLIF(is_flagged, 0) AS status_code_high_per_is_flagged,
        COALESCE(k, 'N/A') AS k_clean
    FROM unpacked
    WHERE k IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY status_code_high NULLS LAST, id, k) AS dedup_rank
        FROM valid
    ) ranked
    WHERE dedup_rank = 1
),
filtered AS (
    SELECT
        id,
        k,
        status_code_high,
        is_flagged,
        k_filled,
        status_code_high_per_is_flagged,
        k_clean,
        ROUND(status_code_high_per_is_flagged * 1.15, 2) AS adjusted_status_code_high_per_is_flagged,
        CASE WHEN status_code_high >= 2 THEN 'High' WHEN status_code_high >= 1 THEN 'Medium' ELSE 'Low' END AS status_code_high_band,
        COALESCE(k, 'NONE') AS resolved_k
    FROM latest
    WHERE k IS NOT NULL
),
enriched AS (
    SELECT
        id,
        k,
        status_code_high,
        k_filled,
        status_code_high_per_is_flagged,
        status_code_high_band,
        COUNT(is_flagged) OVER (PARTITION BY k) AS k_count_is_flagged,
        NTILE(10) OVER (PARTITION BY k ORDER BY adjusted_status_code_high_per_is_flagged DESC, id, k) AS adjusted_status_code_high_per_is_flagged_bucket
    FROM filtered
    WHERE status_code_high > 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY adjusted_status_code_high_per_is_flagged_bucket DESC NULLS FIRST) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
),
daily AS (
    SELECT
        CASE WHEN status_code_high_per_is_flagged >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_per_is_flagged_tier,
        AVG(status_code_high) AS avg_status_code_high,
        COUNT_IF(k = 'region') AS region_events,
        PERCENTILE_APPROX(status_code_high, 0.5) AS status_code_high_p50
    FROM ranked
    GROUP BY CASE WHEN status_code_high_per_is_flagged >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    status_code_high_per_is_flagged_tier,
    avg_status_code_high,
    status_code_high_p50
FROM daily
ORDER BY status_code_high_per_is_flagged_tier DESC NULLS LAST, status_code_high_p50;
