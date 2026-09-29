WITH SupplierTotals AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END) AS electronics_count
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.supplier_name, S.electronics_count
FROM suppliers s
JOIN SupplierTotals S ON s.id = S.supplier_id
WHERE S.electronics_count >= 32
ORDER BY electronics_count;
