WITH valid AS (
    SELECT *, CONCAT(status, '/', plan) AS status_plan
    FROM subscriptions
    WHERE end_date > DATE '2023-10-21'
      AND customer_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY rn, id) AS latest_rank
        FROM latest
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    CASE WHEN monthly_fee >= 259 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
    status,
    MAX(monthly_fee) FILTER (WHERE plan = 'enterprise') AS enterprise_max_monthly_fee,
    SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) AS paused_monthly_fee,
    PERCENTILE_APPROX(monthly_fee, 0.95) AS monthly_fee_p95,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
    GROUPING_ID() AS grouping_id
FROM ranked
GROUP BY CUBE (CASE WHEN monthly_fee >= 259 THEN 'PREMIUM' ELSE 'STANDARD' END, status)
HAVING STDDEV_SAMP(monthly_fee) >= 0
ORDER BY enterprise_max_monthly_fee DESC;
