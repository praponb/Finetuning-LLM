WITH top_orders AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) AS rn FROM orders) SELECT id, customer_id, amount FROM top_orders WHERE rn <= 3 ORDER BY id DESC;
