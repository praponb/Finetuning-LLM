SELECT opened_date FROM stores WHERE opened_date < '2023-03-01' EXCEPT SELECT order_date FROM orders WHERE customer_id = 4 ORDER BY opened_date
