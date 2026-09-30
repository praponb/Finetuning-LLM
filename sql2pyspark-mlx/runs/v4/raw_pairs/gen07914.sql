SELECT
    i.id,
    i.store_id,
    ss.product_id,
    ss.sale_date,
    CAST(ss.units AS STRING) AS units_str
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (3, 4) AND i.last_restock <= '2023-04-25'
