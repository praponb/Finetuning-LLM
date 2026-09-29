SELECT 'account' AS origin, status FROM accounts UNION SELECT 'payment' AS origin, status FROM payments WHERE method = 'card' ORDER BY origin DESC;
