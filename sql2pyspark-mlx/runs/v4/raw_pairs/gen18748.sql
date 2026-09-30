SELECT *, RANK() OVER (PARTITION BY segment, country ORDER BY credit_limit DESC) AS latest_rank
FROM customers
QUALIFY latest_rank = 1;
