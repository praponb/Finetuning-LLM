WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(monthly_fee, 0) AS monthly_fee_filled
    FROM subscriptions
    WHERE start_date >= DATE '2023-08-08'
      AND customer_id IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        monthly_fee_filled,
        TRIM(status) AS clean_status
    FROM valid
    WHERE end_date >= DATE '2023-12-31'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee_filled DESC NULLS FIRST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    CASE WHEN monthly_fee_filled >= 70 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_filled_tier,
    SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee_filled ELSE 0 END) AS expired_monthly_fee_filled,
    APPROX_PERCENTILE(monthly_fee_filled, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_filled_quartiles,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(monthly_fee_filled, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_filled_quartiles_2,
    PERCENTILE_APPROX(monthly_fee_filled, 0.95) AS monthly_fee_filled_p95,
    AVG(monthly_fee_filled) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_monthly_fee_filled
FROM latest
GROUP BY DATE_TRUNC('YEAR', start_date), CASE WHEN monthly_fee_filled >= 70 THEN 'HIGH' ELSE 'LOW' END
ORDER BY enterprise_avg_monthly_fee_filled DESC;
