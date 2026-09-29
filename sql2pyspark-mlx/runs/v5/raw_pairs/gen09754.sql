SELECT id, segment, credit_limit, ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 3) AS credit_limit_share FROM customers WHERE credit_limit <= 8800;
