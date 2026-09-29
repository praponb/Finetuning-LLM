WITH recent_subscriptions AS (
    SELECT *, ROUND(monthly_fee, 1) AS monthly_fee_rounded
    FROM subscriptions
    WHERE customer_id IS NULL AND YEAR(start_date) = 2023
),
start_quarter_summary AS (
    SELECT QUARTER(start_date) AS start_quarter, status, COUNT(monthly_fee_rounded) AS monthly_fee_rounded_count
    FROM recent_subscriptions
    GROUP BY QUARTER(start_date), status
)
SELECT start_quarter, status, monthly_fee_rounded_count
FROM start_quarter_summary
ORDER BY start_quarter ASC;
