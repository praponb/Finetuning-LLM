SELECT status, channel, SUM(shipping_fee) total_shipping_fee FROM orders WHERE customer_id = 12 AND (store_id IS NOT NULL OR discount >= 0.36) GROUP BY status, channel;
