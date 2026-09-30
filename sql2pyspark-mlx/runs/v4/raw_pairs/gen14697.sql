SELECT id, order_id, amount, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num FROM payments WHERE HOUR(paid_at) BETWEEN 7 AND 11;
