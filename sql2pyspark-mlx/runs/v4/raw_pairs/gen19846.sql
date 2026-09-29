SELECT supplier_id, category, unique_suppliers
FROM (
    SELECT supplier_id, category, COUNT(DISTINCT price) AS unique_suppliers
    FROM products
    WHERE category <> 'Fashion'
    GROUP BY supplier_id, category
) AS agg
WHERE unique_suppliers >= 4
ORDER BY category, supplier_id
LIMIT 3;
