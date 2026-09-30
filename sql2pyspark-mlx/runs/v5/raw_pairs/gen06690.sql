SELECT sessions.customer_id, customers.is_active FROM sessions FULL OUTER JOIN customers ON customers.id = sessions.customer_id WHERE sessions.browser != 'Edge' ORDER BY customer_id
