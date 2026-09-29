SELECT status, min_balance, balance_count
FROM (
    SELECT status, MIN(balance) min_balance, COUNT(balance) balance_count
    FROM accounts
    GROUP BY status
) t
WHERE balance_count < 8
ORDER BY 3 DESC;
