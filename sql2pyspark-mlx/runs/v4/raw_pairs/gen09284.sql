SELECT customer_id FROM sessions INTERSECT SELECT customer_id FROM sessions WHERE country NOT IN ('DE', 'SG');
