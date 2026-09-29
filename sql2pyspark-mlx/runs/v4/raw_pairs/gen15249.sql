SELECT * FROM (SELECT status, channel, amount FROM orders WHERE status = 'PENDING')
PIVOT (SUM(amount) FOR channel IN ('web', 'store', 'mobile'))
ORDER BY status
