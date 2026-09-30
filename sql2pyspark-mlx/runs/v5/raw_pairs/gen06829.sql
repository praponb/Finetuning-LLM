SELECT channel, MAX(amount) - MIN(amount) AS amount_range, MAX(shipping_fee) AS max_shipping_fee FROM orders WHERE store_id = 32 GROUP BY channel ORDER BY 3, channel DESC;
