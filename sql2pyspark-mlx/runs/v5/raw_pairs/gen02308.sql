SELECT customer_id FROM orders WHERE channel IN ('mobile', 'web', 'store') INTERSECT SELECT customer_id FROM orders WHERE customer_id = 11 ORDER BY customer_id;
