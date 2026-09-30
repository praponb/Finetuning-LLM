WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(DISTINCT cost) unique_brands, MIN(price) min_price
    FROM products
    WHERE cost < 680
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    pbs.unique_brands,
    pbs.min_price
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.unique_brands >= 9
ORDER BY id DESC;
