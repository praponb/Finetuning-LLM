SELECT id, supplier_name, CASE WHEN city = 'London' THEN 1 ELSE 0 END is_london FROM suppliers WHERE rating <= 3.7 AND country <> 'DE'
