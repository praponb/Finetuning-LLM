SELECT * FROM (SELECT channel, status, discount FROM orders) PIVOT (MAX(discount) FOR status IN ('SHIPPED', 'COMPLETED')) ORDER BY channel
