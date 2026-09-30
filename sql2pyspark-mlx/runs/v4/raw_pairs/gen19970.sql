SELECT id, country, rating, supplier_name, SUBSTRING(country, 1, 3) AS country_prefix FROM suppliers WHERE NOT (supplier_name LIKE '%Wholes%' AND rating <= 3.3) ORDER BY country, id LIMIT 5;
