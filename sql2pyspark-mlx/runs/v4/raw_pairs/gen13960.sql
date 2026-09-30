WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, attr_value
),
parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        attr_key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM exploded
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY attr_key ORDER BY status_code_high) AS status_code_high_cume_dist,
        COUNT(status_code_high) OVER (PARTITION BY attr_key) AS attr_key_count_status_code_high,
        SUM(status_code_high) OVER (ORDER BY status_code_high, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code_high
    FROM parsed
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (ORDER BY status_code_high_cume_dist, id, attr_key) AS status_code_high_cume_dist_bucket,
        ROUND(attr_key_count_status_code_high / SUM(attr_key_count_status_code_high) OVER (PARTITION BY attr_key), 4) AS attr_key_count_status_code_high_share,
        LAG(status_code_high_cume_dist, 2) OVER w_grp AS prev_status_code_high_cume_dist,
        NTILE(4) OVER (PARTITION BY attr_key ORDER BY status_code_high_cume_dist DESC, id, attr_key) AS status_code_high_cume_dist_bucket_2
    FROM scored
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY flag_bit2, id, attr_key)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high_cume_dist_bucket_2 NULLS FIRST) AS dedup_rank
        FROM with_windows
    ) t
    WHERE dedup_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_high_cume_dist_bucket_2 >= 8 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_cume_dist_bucket_2_tier,
    CASE WHEN status_code_high_cume_dist_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_cume_dist_bucket_tier,
    CASE WHEN attr_key_count_status_code_high >= 15 THEN 'HIGH' ELSE 'LOW' END AS attr_key_count_status_code_high_tier,
    APPROX_PERCENTILE(attr_key_count_status_code_high, 0.9) AS attr_key_count_status_code_high_p90,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(status_code_high_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_cume_dist_quartiles,
    ROUND(SUM(status_code_high_cume_dist) / NULLIF(COUNT(CASE WHEN attr_key = 'region' THEN 1 END), 0), 2) AS status_code_high_cume_dist_per_region,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys_2
FROM deduped
GROUP BY CUBE (CASE WHEN status_code_high_cume_dist_bucket_2 >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_code_high_cume_dist_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN attr_key_count_status_code_high >= 15 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY event_count NULLS FIRST;
