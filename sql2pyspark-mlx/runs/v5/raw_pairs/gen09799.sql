SELECT
    stores.region,
    store_sales.store_id,
    store_sales.product_id,
    ROUND(stores.sqft * 1.15, 2) AS sqft_scaled
FROM stores
INNER JOIN store_sales ON stores.id = store_sales.store_id;
