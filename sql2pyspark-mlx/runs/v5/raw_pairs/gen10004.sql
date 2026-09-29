WITH products_by_supplier AS (
    SELECT supplier_id, SUM(cost) AS total_cost, SUM(CASE WHEN brand = 'Initech' THEN cost ELSE 0 END) AS initech_cost
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'premium')
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    pbs.total_cost,
    pbs.initech_cost
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY s.supplier_name, id
LIMIT 5;
