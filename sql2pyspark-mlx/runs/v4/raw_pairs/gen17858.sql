SELECT id, store_name, opened_date, CASE WHEN city = 'London' THEN 1 ELSE 0 END AS is_london FROM stores
