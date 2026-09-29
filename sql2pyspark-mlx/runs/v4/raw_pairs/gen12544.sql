WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS latest_rank
        FROM subscriptions
    ) x
    WHERE latest_rank = 1
)
SELECT
    CASE WHEN monthly_fee >= 269 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    customer_id,
    AVG(monthly_fee) FILTER (WHERE plan = 'basic') AS basic_avg_monthly_fee,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    APPROX_PERCENTILE(monthly_fee, 0.9) AS monthly_fee_p90,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    GROUPING_ID() AS grouping_id
FROM ranked
GROUP BY GROUPING SETS ((CASE WHEN monthly_fee >= 269 THEN 'HIGH' ELSE 'LOW' END, customer_id), (CASE WHEN monthly_fee >= 269 THEN 'HIGH' ELSE 'LOW' END));
