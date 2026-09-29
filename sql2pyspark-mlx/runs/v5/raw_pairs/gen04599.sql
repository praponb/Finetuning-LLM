SELECT
    status,
    account_type,
    ROUND(AVG(balance), 2) AS avg_balance,
    MAX(balance) AS max_balance
FROM accounts
WHERE status IN ('OPEN', 'FROZEN') AND status IS NOT NULL AND opened_date < '2025-05-27'
GROUP BY status, account_type
HAVING max_balance < 36000;
