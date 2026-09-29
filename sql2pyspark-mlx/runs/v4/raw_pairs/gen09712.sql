SELECT id, amount, store_id, channel, discount + shipping_fee AS discount_plus_shipping_fee, amount / NULLIF(discount, 0) AS amount_per_discount FROM orders;
