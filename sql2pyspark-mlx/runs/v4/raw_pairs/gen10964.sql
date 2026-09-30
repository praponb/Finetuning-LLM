WITH account_stats AS (
    SELECT account_id, COUNT(DISTINCT amount) AS unique_accounts, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.customer_id,
    a.balance,
    as.unique_accounts,
    as.stddev_amount
FROM accounts a
INNER JOIN account_stats as ON a.id = as.account_id
ORDER BY stddev_amount DESC;
