SELECT
    status,
    CASE WHEN balance >= 88000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    MIN(balance) AS min_balance,
    COUNT_IF(status = 'CLOSED') AS closed_events,
    COUNT_IF(status = 'CLOSED') AS closed_events_2,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END), 0), 2) AS balance_per_credit,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count
FROM accounts
GROUP BY 1, 2
HAVING balance_per_credit < 0
