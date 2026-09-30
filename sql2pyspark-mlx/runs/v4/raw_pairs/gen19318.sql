SELECT
    plan,
    status,
    SUM(CASE WHEN plan = 'standard' THEN monthly_fee ELSE 0 END) AS standard_monthly_fee,
    SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count
FROM subscriptions
WHERE monthly_fee IS NOT NULL AND monthly_fee < 170 AND plan = 'premium'
GROUP BY plan, status
ORDER BY plan;
