SELECT id, customer_id, discount FROM (SELECT id, customer_id, discount, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS rn FROM orders) AS ranked WHERE rn = 1;
