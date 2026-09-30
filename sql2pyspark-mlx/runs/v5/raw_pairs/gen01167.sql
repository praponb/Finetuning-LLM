SELECT 'order_item' source, unit_price value FROM order_items UNION ALL SELECT 'payment' source, amount value FROM payments WHERE method = 'paypal'
