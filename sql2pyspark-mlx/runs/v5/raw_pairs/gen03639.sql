SELECT
    store_sales.id,
    store_sales.product_id,
    stores.store_name,
    products.category,
    ROUND(store_sales.units, 1) AS units_rounded
FROM store_sales
INNER JOIN stores ON store_sales.store_id = stores.id
LEFT JOIN products ON store_sales.product_id = products.id
WHERE LENGTH(stores.store_name) < 13
ORDER BY id DESC, units_rounded
LIMIT 20;
