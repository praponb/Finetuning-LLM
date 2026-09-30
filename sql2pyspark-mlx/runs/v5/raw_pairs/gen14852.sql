WITH products_by_supplier AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_umbrella, COUNT(*) AS num_products
    FROM products
    WHERE supplier_id IS NOT NULL
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.supplier_name,
    pbs.pct_umbrella,
    pbs.num_products
FROM suppliers s
LEFT OUTER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY supplier_name ASC;
