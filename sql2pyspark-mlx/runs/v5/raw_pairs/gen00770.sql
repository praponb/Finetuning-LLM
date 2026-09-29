SELECT
    account_type,
    ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND((100 * SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_savings
FROM accounts
WHERE opened_date > '2023-09-02' AND account_type IN ('CREDIT', 'SAVINGS')
GROUP BY account_type;
