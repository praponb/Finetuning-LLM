SELECT a.status, t.unique_merchants
FROM accounts a
INNER JOIN (SELECT account_id, COUNT(DISTINCT amount) AS unique_merchants
    FROM transactions
    WHERE account_id = 35
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.unique_merchants > 8
ORDER BY unique_merchants ASC
