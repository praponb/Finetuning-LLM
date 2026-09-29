WITH flattened AS (
    SELECT
        id,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
valid AS (
    SELECT *, ROUND(status_code * 1.1, 2) AS adjusted_status_code
    FROM flattened
    WHERE k IS NOT NULL
),
scored AS (
    SELECT
        *,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k), 4) AS status_code_share,
        RANK() OVER (PARTITION BY k ORDER BY adjusted_status_code ASC) AS adjusted_status_code_rank,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY k), 4) AS status_code_share_2
    FROM valid
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY k ORDER BY adjusted_status_code DESC NULLS LAST) AS adjusted_status_code_cume_dist,
        COUNT(adjusted_status_code) OVER w_ordered AS k_count_adjusted_status_code,
        MIN(adjusted_status_code_rank) OVER w_ordered AS k_min_adjusted_status_code_rank,
        ROUND(adjusted_status_code_rank / SUM(adjusted_status_code_rank) OVER (PARTITION BY k), 4) AS adjusted_status_code_rank_share
    FROM scored
    WINDOW w_ordered AS (PARTITION BY k)
),
with_windows AS (
    SELECT
        id,
        status_code,
        k,
        value,
        adjusted_status_code,
        status_code_share,
        adjusted_status_code_rank,
        status_code_share_2,
        adjusted_status_code_cume_dist,
        k_count_adjusted_status_code,
        k_min_adjusted_status_code_rank,
        adjusted_status_code_rank_share,
        AVG(adjusted_status_code_rank) OVER (PARTITION BY k) AS k_avg_adjusted_status_code_rank,
        SUM(adjusted_status_code_rank_share) OVER (PARTITION BY k) AS k_sum_adjusted_status_code_rank_share
    FROM metrics
),
summary AS (
    SELECT
        k,
        CASE WHEN adjusted_status_code >= 72 THEN 'HIGH' ELSE 'LOW' END AS adjusted_status_code_tier,
        AVG(status_code) FILTER (WHERE k = 'region') AS region_avg_status_code,
        SIZE(COLLECT_SET(k)) AS num_ks,
        SIZE(COLLECT_SET(k)) AS num_ks_2,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY SIZE(COLLECT_SET(k)) DESC) AS num_ks_percent_rank
    FROM with_windows
    GROUP BY k, CASE WHEN adjusted_status_code >= 72 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    k,
    adjusted_status_code_tier,
    region_avg_status_code,
    num_ks,
    num_ks_2,
    num_ks_percent_rank
FROM summary;
