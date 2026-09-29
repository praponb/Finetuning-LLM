SELECT id, customer_id FROM sessions INTERSECT SELECT id, customer_id FROM sessions WHERE browser = 'Edge'
