WITH ranked_orders AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_num FROM orders) SELECT id, status, amount FROM ranked_orders WHERE row_num <= 5;
