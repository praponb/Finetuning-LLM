SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount NULLS LAST, id) AS rn
FROM orders
QUALIFY rn = 1;
