SELECT * FROM (SELECT channel, status, amount FROM orders) PIVOT (AVG(amount) FOR status IN ('SHIPPED', 'CANCELLED')) ORDER BY channel
