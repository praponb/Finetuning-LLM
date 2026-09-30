SELECT o.status, o.amount, s.opened_date, s.sqft FROM orders o FULL OUTER JOIN stores s ON o.store_id = s.id
