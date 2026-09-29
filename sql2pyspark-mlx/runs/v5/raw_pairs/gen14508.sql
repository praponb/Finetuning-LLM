SELECT balance_bucket, avg_balance, first_opened_date
FROM (
    SELECT CASE WHEN balance >= 71000 THEN 'high' WHEN balance >= 23000 THEN 'mid' ELSE 'low' END AS balance_bucket, AVG(balance) AS avg_balance, MIN(opened_date) AS first_opened_date
    FROM accounts
    GROUP BY CASE WHEN balance >= 71000 THEN 'high' WHEN balance >= 23000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE avg_balance < 73000
ORDER BY balance_bucket DESC, avg_balance;
