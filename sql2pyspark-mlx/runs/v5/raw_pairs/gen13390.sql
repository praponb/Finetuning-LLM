SELECT * FROM (SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, status, amount FROM orders) PIVOT (COUNT(amount) FOR status IN ('SHIPPED', 'CANCELLED', 'RETURNED')) ORDER BY order_month
