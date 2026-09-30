WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(DISTINCT stock_qty) unique_categorys
    FROM products
    WHERE stock_qty >= 321
    GROUP BY supplier_id
)
SELECT s.supplier_name, s.city, pbs.unique_categorys
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.unique_categorys >= 9
ORDER BY unique_categorys;
