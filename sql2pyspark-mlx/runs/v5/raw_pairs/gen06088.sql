SELECT
    status,
    SUM(monthly_fee) AS total_monthly_fee,
    COUNT(*) AS cnt,
    ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
WHERE start_date >= '2024-02-09'
GROUP BY status
ORDER BY total_monthly_fee;
