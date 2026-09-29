SELECT browser, customer_id FROM sessions EXCEPT DISTINCT SELECT browser, customer_id FROM sessions WHERE device = 'tablet';
