WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(monthly_fee * 1.1, 2) AS monthly_fee_adjusted,
        ADD_MONTHS(start_date, 1) AS start_date_plus_months
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-20'
      AND end_date <= '2024-08-01'
)
-- Join a dimension and aggregate
SELECT
    c.segment,
    s.plan,
    COUNT(*) AS num_rows,
    AVG(s.monthly_fee_adjusted) AS avg_monthly_fee_adjusted,
    MAX(s.monthly_fee) AS max_monthly_fee
FROM valid s
JOIN customers c ON s.customer_id = c.id
GROUP BY ROLLUP (c.segment, s.plan);
