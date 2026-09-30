SELECT
    stores.id,
    stores.sqft,
    stores.store_name,
    t.min_revenue
FROM stores
LEFT JOIN (SELECT store_id, MIN(revenue) AS min_revenue
    FROM store_sales
    GROUP BY store_id) t ON stores.id = t.store_id
ORDER BY stores.sqft DESC, stores.store_name ASC
