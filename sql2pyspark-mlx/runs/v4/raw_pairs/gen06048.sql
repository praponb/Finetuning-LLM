SELECT id, balance, DENSE_RANK() OVER (ORDER BY balance) dense_rnk
FROM accounts
ORDER BY id DESC;
