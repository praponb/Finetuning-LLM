SELECT status, monthly_fee_range, basic_monthly_fee
FROM (
    SELECT status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range, SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee
    FROM subscriptions
    GROUP BY status
) AS s
WHERE basic_monthly_fee >= 26
ORDER BY 2 DESC;
