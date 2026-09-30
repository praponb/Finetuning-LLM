WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) pos FROM customers) SELECT id, segment, credit_limit FROM ranked WHERE pos <= 3 ORDER BY id DESC, credit_limit
