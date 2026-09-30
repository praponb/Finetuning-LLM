WITH SupplierTotals AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_umbrella
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    s.country,
    S.pct_umbrella
FROM suppliers s
LEFT JOIN SupplierTotals S ON s.id = S.supplier_id
WHERE S.pct_umbrella > 79
