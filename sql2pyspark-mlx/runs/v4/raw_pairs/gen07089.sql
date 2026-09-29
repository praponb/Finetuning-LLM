SELECT MAX(amount) - MIN(amount) amount_range FROM payments WHERE order_id IS NOT NULL ORDER BY amount_range DESC;
