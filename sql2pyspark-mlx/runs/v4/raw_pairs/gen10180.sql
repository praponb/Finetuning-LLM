SELECT end_quarter, avg_monthly_fee, enterprise_count
FROM (
    SELECT QUARTER(end_date) AS end_quarter, AVG(monthly_fee) AS avg_monthly_fee, COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
    FROM subscriptions
    WHERE status <> 'PAUSED'
    GROUP BY QUARTER(end_date)
) AS s
WHERE avg_monthly_fee >= 160;
