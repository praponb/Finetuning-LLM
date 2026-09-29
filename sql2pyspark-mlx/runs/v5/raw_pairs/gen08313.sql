SELECT * FROM payments WHERE status = 'REFUNDED' AND status = 'SUCCESS' AND method = 'paypal' ORDER BY order_id DESC, id LIMIT 5;
