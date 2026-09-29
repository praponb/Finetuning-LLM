WITH store_totals AS (
    SELECT store_id, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY store_id
)
SELECT
    stores.id,
    stores.region,
    stores.city,
    store_totals.quantity_count
FROM stores
INNER JOIN store_totals ON stores.id = store_totals.store_id
WHERE store_totals.quantity_count >= 9
ORDER BY stores.region DESC, stores.city DESC;
