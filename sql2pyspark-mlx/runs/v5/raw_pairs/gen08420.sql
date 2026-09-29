WITH lines AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
flattened_attrs AS (
    SELECT
        host,
        latency_ms,
        status_code,
        attr_key,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
filtered AS (
    SELECT
        *,
        status_code / NULLIF(is_flagged, 0) AS status_code_per_is_flagged,
        LEAST(is_flagged, latency_ms) AS least_is_flagged_latency_ms,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled
    FROM flattened_attrs
    WHERE attr_key IS NOT NULL
),
scored AS (
    SELECT
        *,
        SUM(status_code_per_is_flagged) OVER (PARTITION BY host) AS host_sum_status_code_per_is_flagged,
        CUME_DIST() OVER (ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_cume_dist
    FROM filtered
),
scored_app_logs AS (
    SELECT
        latency_ms,
        status_code,
        attr_key,
        status_code_per_is_flagged,
        least_is_flagged_latency_ms,
        host_sum_status_code_per_is_flagged,
        is_flagged_cume_dist,
        CUME_DIST() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist,
        CORR(latency_ms, status_code) OVER (PARTITION BY host, attr_key) AS corr_latency_ms_status_code,
        CUME_DIST() OVER (PARTITION BY host ORDER BY is_flagged_cume_dist DESC NULLS LAST) AS is_flagged_cume_dist_cume_dist
    FROM scored
),
enriched AS (
    SELECT
        latency_ms,
        status_code,
        status_code_per_is_flagged,
        host_sum_status_code_per_is_flagged,
        latency_ms_cume_dist,
        corr_latency_ms_status_code,
        CUME_DIST() OVER (ORDER BY corr_latency_ms_status_code DESC NULLS FIRST) AS corr_latency_ms_status_code_cume_dist,
        CORR(status_code, latency_ms) OVER (PARTITION BY attr_key) AS corr_status_code_latency_ms,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY is_flagged_cume_dist DESC NULLS FIRST) AS is_flagged_cume_dist_dense_rank
    FROM scored_app_logs
),
scored_app_logs_2 AS (
    SELECT
        status_code,
        status_code_per_is_flagged,
        host_sum_status_code_per_is_flagged,
        latency_ms_cume_dist,
        corr_latency_ms_status_code,
        corr_latency_ms_status_code_cume_dist,
        is_flagged_cume_dist_dense_rank,
        PERCENT_RANK() OVER (ORDER BY is_flagged_cume_dist_dense_rank NULLS FIRST) AS is_flagged_cume_dist_dense_rank_percent_rank
    FROM enriched
    WHERE status_code_per_is_flagged >= 28
)
SELECT
    CASE WHEN is_flagged_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_flagged_cume_dist_dense_rank_tier,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2
FROM scored_app_logs_2
GROUP BY CUBE (CASE WHEN is_flagged_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING cnt < 1000
