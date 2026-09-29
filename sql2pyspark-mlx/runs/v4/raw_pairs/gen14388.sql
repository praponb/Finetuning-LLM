SELECT c.city, MIN(o.order_date) AS first_order_date FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE c.age <= 54 GROUP BY c.city;
