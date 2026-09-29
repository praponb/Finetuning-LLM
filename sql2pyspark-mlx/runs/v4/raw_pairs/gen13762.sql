SELECT * FROM (SELECT channel, status, amount FROM orders WHERE shipping_fee < 36)
PIVOT (SUM(amount) FOR status IN ('RETURNED', 'COMPLETED', 'CANCELLED', 'SHIPPED'))
