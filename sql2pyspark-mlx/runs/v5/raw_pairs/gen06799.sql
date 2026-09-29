SELECT
    id,
    plan,
    start_date,
    status,
    LAST_DAY(start_date) AS start_date_month_end,
    CAST(monthly_fee AS INT) AS monthly_fee_int
FROM subscriptions
WHERE customer_id = 34 AND YEAR(end_date) >= 2024;
