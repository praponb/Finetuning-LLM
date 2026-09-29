WITH ranked AS (SELECT id, segment, credit_limit, DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) row_num FROM customers) SELECT * FROM ranked WHERE row_num <= 5;
