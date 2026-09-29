SELECT
    store_sales.id,
    store_sales.store_id,
    store_sales.units,
    products.price,
    products.supplier_id,
    suppliers.city
FROM store_sales
JOIN products ON products.id = store_sales.product_id
JOIN suppliers ON products.supplier_id = suppliers.id
WHERE store_sales.revenue < 1700
ORDER BY 2, id
LIMIT 3;
