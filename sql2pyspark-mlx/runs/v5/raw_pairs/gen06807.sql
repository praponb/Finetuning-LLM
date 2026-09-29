SELECT * FROM (SELECT status, customer_id, channel, shipping_fee FROM orders) src PIVOT (SUM(shipping_fee) FOR channel IN ('partner', 'store'));
