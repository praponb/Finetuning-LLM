SELECT
    plan,
    MAX(monthly_fee) AS max_monthly_fee,
    MIN(start_date) AS min_start_date,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
GROUP BY plan
