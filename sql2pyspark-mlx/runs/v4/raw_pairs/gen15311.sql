SELECT o.id, o.discount, o.amount, s.city, s.sqft, c.country FROM orders o RIGHT OUTER JOIN stores s ON o.store_id = s.id JOIN customers c ON o.customer_id = c.id ORDER BY o.discount
