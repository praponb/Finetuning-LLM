SELECT s.rating, s.country, p.brand
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
WHERE NOT (MONTH(p.launch_date) IN (2, 10, 11) AND s.country <> 'TH')
ORDER BY rating DESC;
