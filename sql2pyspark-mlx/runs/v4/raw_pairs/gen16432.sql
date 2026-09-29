WITH products_by_supplier AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) category_list
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.supplier_name, pbs.category_list
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY s.id DESC
