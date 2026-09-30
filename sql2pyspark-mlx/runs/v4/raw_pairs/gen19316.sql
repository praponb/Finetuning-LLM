SELECT * FROM (SELECT QUARTER(order_date) AS order_quarter, channel, shipping_fee FROM orders WHERE store_id = 10)
PIVOT (AVG(shipping_fee) FOR channel IN ('mobile', 'partner', 'store', 'web'));
