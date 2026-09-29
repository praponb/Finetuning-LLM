WITH inventory_by_store AS (
    SELECT store_id, MIN(quantity) AS min_quantity
    FROM inventory
    WHERE quantity BETWEEN 150 AND 196
    GROUP BY store_id
)
SELECT stores.city, stores.sqft, inventory_by_store.min_quantity
FROM stores
JOIN inventory_by_store ON stores.id = inventory_by_store.store_id
WHERE inventory_by_store.min_quantity > 220;
