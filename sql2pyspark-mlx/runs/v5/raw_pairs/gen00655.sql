SELECT id, balance, DENSE_RANK() OVER (ORDER BY balance) AS dense_rnk
FROM accounts
