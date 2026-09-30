SELECT id, supplier_name, city, country, CASE WHEN rating >= 3.7 THEN 'High' WHEN rating >= 2.1 THEN 'Medium' ELSE 'Low' END AS rating_level FROM suppliers;
