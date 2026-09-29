SELECT customers.is_active, customers.country, MAX(orders.order_date) AS last_order_date
FROM orders
JOIN customers ON orders.customer_id = customers.id
WHERE customers.is_active
GROUP BY customers.is_active, customers.country
ORDER BY customers.country;
