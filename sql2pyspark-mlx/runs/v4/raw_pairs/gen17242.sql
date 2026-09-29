SELECT *, NTILE(10) OVER (ORDER BY amount DESC, id) AS amount_bucket, LEAD(shipping_fee, 1) OVER (PARTITION BY channel, store_id ORDER BY amount, id) AS next_shipping_fee FROM orders
