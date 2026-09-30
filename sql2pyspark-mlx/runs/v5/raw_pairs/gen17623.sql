SELECT * FROM (SELECT status, channel, shipping_fee FROM orders) PIVOT (AVG(shipping_fee) FOR channel IN ('web', 'store', 'partner', 'mobile'));
