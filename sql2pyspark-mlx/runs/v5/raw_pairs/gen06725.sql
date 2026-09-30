SELECT o.id, o.amount, c.segment, s.sqft, s.opened_date, TRIM(o.channel) AS clean_channel FROM orders o LEFT JOIN customers c ON o.customer_id = c.id LEFT JOIN stores s ON o.store_id = s.id
