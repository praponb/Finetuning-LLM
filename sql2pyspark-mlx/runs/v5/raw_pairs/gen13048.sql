SELECT id, channel, shipping_fee, order_date, CAST(shipping_fee AS INT) AS shipping_fee_int FROM orders WHERE discount BETWEEN 0.16 AND 0.39 AND shipping_fee < 29
