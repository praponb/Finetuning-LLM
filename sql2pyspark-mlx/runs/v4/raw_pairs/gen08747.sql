SELECT *
FROM (
    SELECT status, SUM(balance) AS total_balance
    FROM accounts
    GROUP BY status
) AS agg
WHERE total_balance >= 297000
ORDER BY status;
