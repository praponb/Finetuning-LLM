SELECT
    store_sales.id,
    store_sales.revenue,
    stores.sqft,
    stores.region,
    products.launch_date
FROM store_sales
LEFT JOIN stores ON stores.id = store_sales.store_id
JOIN products ON store_sales.product_id = products.id
ORDER BY region;
