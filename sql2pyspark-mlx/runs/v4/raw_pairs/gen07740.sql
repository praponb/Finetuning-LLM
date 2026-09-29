SELECT browser, customer_id FROM sessions INTERSECT SELECT browser, customer_id FROM sessions WHERE device = 'desktop';
