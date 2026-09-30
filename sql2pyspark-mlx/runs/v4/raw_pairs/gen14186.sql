SELECT id, order_id, amount FROM (SELECT id, order_id, amount, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS rn FROM payments WHERE paid_at < '2024-08-01') AS ranked WHERE rn <= 2;
