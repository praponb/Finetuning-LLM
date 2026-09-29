SELECT id, channel, store_id, discount / NULLIF(shipping_fee, 0) discount_per_shipping_fee FROM orders WHERE channel = 'mobile'
