SELECT * FROM (SELECT store_id, status, channel, amount FROM orders) src PIVOT (SUM(amount) AS total, COUNT(1) AS cnt FOR channel IN ('web', 'mobile', 'partner'))
