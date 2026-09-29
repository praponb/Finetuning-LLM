SELECT
    status,
    plan,
    pct_standard,
    active_monthly_fee
FROM (
    SELECT
        status,
        plan,
        ROUND((100 * SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_standard,
        SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee ELSE 0 END) AS active_monthly_fee
    FROM subscriptions
    WHERE status = 'CANCELLED'
    GROUP BY status, plan
) AS x
WHERE pct_standard > 26
ORDER BY plan;
