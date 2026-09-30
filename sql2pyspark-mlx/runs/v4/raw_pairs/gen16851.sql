WITH scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_dense_rank,
        SUM(monthly_fee) OVER (PARTITION BY plan, status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM subscriptions
)
SELECT
    CASE WHEN monthly_fee_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_dense_rank_tier,
    DATE_TRUNC('WEEK', end_date) AS week_start,
    SUM(cumulative_monthly_fee) AS total_cumulative_monthly_fee,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    MIN(cumulative_monthly_fee) AS min_cumulative_monthly_fee,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(monthly_fee_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_dense_rank_quartiles
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN monthly_fee_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', end_date)), (CASE WHEN monthly_fee_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END), ());
