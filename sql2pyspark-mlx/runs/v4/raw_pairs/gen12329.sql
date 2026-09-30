SELECT * FROM (SELECT status, channel, shipping_fee FROM orders)
PIVOT (COUNT(shipping_fee) FOR channel IN ('mobile', 'store', 'web'))
ORDER BY status
