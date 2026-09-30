SELECT * FROM (SELECT store_id, channel, amount FROM orders) src PIVOT (MAX(amount) AS total, COUNT(1) AS item_count FOR channel IN ('partner', 'store', 'web'))
