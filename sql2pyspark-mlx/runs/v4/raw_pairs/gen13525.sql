SELECT
    plan,
    QUARTER(start_date) AS start_quarter,
    SUM(monthly_fee) AS total_monthly_fee,
    MAX(end_date) AS latest_end_date
FROM subscriptions
WHERE end_date > '2023-03-01'
GROUP BY plan, QUARTER(start_date)
