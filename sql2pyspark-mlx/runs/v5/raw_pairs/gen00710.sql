WITH lines AS (
    SELECT
        id,
        log_ts,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, v
),
flattened_attrs AS (
    SELECT
        id,
        status_code,
        k,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(status_code_high, TRUE) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_code_high,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY is_flagged NULLS LAST) AS is_flagged_dense_rank
    FROM flattened_attrs
    WHERE k NOT IN ('region', 'version')
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(last_known_status_code_high) OVER w_recent AS k_stddev_samp_last_known_status_code_high,
        SUM(status_code) OVER (ORDER BY is_flagged, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_code,
        RANK() OVER (PARTITION BY k ORDER BY is_flagged_dense_rank NULLS LAST) AS is_flagged_dense_rank_rank
    FROM enriched
    WINDOW w_recent AS (PARTITION BY k)
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY k ORDER BY running_status_code DESC) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
)
SELECT
    k,
    CASE WHEN is_flagged_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_flagged_dense_rank_tier,
    COUNT(CASE WHEN k = 'region' THEN 1 END) AS region_count,
    ROUND(AVG(last_known_status_code_high), 2) AS avg_last_known_status_code_high,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest
GROUP BY k, CASE WHEN is_flagged_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_last_known_status_code_high NULLS LAST;
