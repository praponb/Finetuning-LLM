WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM lines
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level ORDER BY is_flagged DESC NULLS LAST) AS latest_rank
        FROM unpacked
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY host ORDER BY status_code_high DESC, id, key) AS status_code_high_bucket,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_dense_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY is_flagged NULLS FIRST) AS is_flagged_cume_dist,
        LEAD(status_code, 1) OVER w_part AS next_status_code
    FROM latest
    WINDOW w_part AS (ORDER BY latest_rank, id, key)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_flagged_cume_dist >= 0.74 THEN 'LARGE' ELSE 'SMALL' END AS is_flagged_cume_dist_tier,
    CASE WHEN is_flagged >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_flagged_tier,
    COUNT(*) FILTER (WHERE host = 'h1') AS h1_rows,
    SUM(CASE WHEN service = 'auth' THEN status_code_high ELSE 0 END) AS auth_status_code_high,
    COUNT(CASE WHEN key = 'region' THEN 1 END) AS region_count,
    MAX(next_status_code) AS max_next_status_code
FROM enriched
GROUP BY CASE WHEN is_flagged_cume_dist >= 0.74 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN is_flagged >= 1 THEN 'HIGH' ELSE 'LOW' END
ORDER BY is_flagged_cume_dist_tier;
