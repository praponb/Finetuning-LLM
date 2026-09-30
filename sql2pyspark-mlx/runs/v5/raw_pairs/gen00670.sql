SELECT rating, CEIL(rating) AS rating_whole, CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 2.1 THEN 'Medium' ELSE 'Low' END AS rating_level
FROM suppliers
WHERE supplier_name LIKE '%Who%' AND rating > 4.0;
