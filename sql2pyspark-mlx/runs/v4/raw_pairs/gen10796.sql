SELECT
    CASE WHEN balance >= 59000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    status,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    COUNT_IF(status = 'CLOSED') AS closed_events
FROM accounts
GROUP BY GROUPING SETS ((CASE WHEN balance >= 59000 THEN 'HIGH' ELSE 'LOW' END, status, DATE_TRUNC('MONTH', opened_date)), (CASE WHEN balance >= 59000 THEN 'HIGH' ELSE 'LOW' END, status), ())
ORDER BY status DESC;
