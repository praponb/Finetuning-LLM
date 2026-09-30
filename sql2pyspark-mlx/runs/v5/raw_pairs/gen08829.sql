SELECT CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'priority' ELSE 'other' END AS status_group, SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee
FROM subscriptions
WHERE monthly_fee BETWEEN 100 AND 230
GROUP BY status_group
HAVING enterprise_monthly_fee < 45;
