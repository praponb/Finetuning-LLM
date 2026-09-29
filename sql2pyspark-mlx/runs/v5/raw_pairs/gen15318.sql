SELECT s.id, s.supplier_name
FROM suppliers s
WHERE UPPER(TRIM(s.supplier_name)) = 'TOKYO TRADERS' AND s.id IN (SELECT supplier_id FROM products WHERE ARRAY_CONTAINS(tags, 'new'))
ORDER BY s.id
LIMIT 20;
