SELECT id, country, COALESCE(rating, 0) rating_filled
FROM suppliers
WHERE supplier_name LIKE '%Par%'
ORDER BY id
