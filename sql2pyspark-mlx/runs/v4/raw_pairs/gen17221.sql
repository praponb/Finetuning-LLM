SELECT method FROM payments WHERE method = 'promptpay' UNION SELECT method FROM payments WHERE order_id = 25;
