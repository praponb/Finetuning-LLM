SELECT id, method, ROUND(amount, 1) AS amount_rounded FROM payments WHERE amount > 950 AND order_id IS NOT NULL ORDER BY method DESC, id DESC LIMIT 10;
