SELECT unit_price FROM order_items INTERSECT SELECT amount FROM payments ORDER BY unit_price
