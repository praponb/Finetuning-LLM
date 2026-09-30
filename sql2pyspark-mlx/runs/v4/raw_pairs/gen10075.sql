SELECT id, city, supplier_name, INITCAP(TRIM(city)) AS city_title, COALESCE(city, 'unassigned') AS city_filled FROM suppliers
