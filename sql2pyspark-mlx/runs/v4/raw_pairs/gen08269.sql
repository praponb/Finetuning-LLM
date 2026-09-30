SELECT customer_id FROM sessions WHERE country = 'UK' UNION SELECT customer_id FROM sessions WHERE country != 'DE' ORDER BY customer_id;
