SELECT country, 'supplier' AS source FROM suppliers WHERE supplier_name LIKE '%Suppl%' UNION SELECT country, 'session' AS source FROM sessions
