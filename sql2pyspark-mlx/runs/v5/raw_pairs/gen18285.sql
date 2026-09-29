SELECT * FROM (SELECT store_id, channel, discount FROM orders) src PIVOT (SUM(discount) FOR channel IN ('web', 'store', 'mobile'))
