SELECT *
FROM suppliers
WHERE city IN ('Chiang Mai', 'Berlin')
ORDER BY city DESC, supplier_name, id
LIMIT 10;
