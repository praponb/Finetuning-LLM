select store_id, status, least(shipping_fee, amount) as least_shipping_fee_amount, substring(channel, 1, 3) as channel_prefix from orders where customer_id in (4, 14, 17, 37)
