WITH SupplierTotals AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.rating,
    S.brand_list
FROM suppliers s
LEFT OUTER JOIN SupplierTotals S ON s.id = S.supplier_id
ORDER BY s.country ASC, s.rating DESC
