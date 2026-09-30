SELECT * FROM (SELECT customer_id, channel, amount FROM orders) src PIVOT (AVG(amount) AS value_sum, COUNT(1) AS cnt FOR channel IN ('web' AS web, 'store' AS store));
