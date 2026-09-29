SELECT
    store_sales.id,
    store_sales.revenue,
    stores.opened_date,
    stores.sqft,
    products.category
FROM store_sales
FULL OUTER JOIN stores ON stores.id = store_sales.store_id
LEFT OUTER JOIN products ON products.id = store_sales.product_id;
