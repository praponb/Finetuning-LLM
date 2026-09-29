WITH products_by_supplier AS (
    SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) AS acme_cost
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    s.country,
    pbs.brand_values,
    pbs.acme_cost
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY brand_values DESC;
