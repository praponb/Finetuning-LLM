SELECT id, order_id, COALESCE(amount, 0) amount_filled FROM payments WHERE order_id IS NOT NULL OR order_id IS NULL;
