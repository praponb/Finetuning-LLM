SELECT * FROM (SELECT status, channel, amount FROM orders)
PIVOT (AVG(amount) FOR channel IN ('mobile', 'store', 'partner'))
ORDER BY status;
