SELECT id, channel, shipping_fee, amount, amount + shipping_fee AS amount_with_shipping FROM orders WHERE customer_id IS NULL AND amount >= 1400 AND store_id IN (20, 32) ORDER BY channel, amount
