SELECT
    stores.city,
    stores.store_name,
    stores.opened_date,
    orders.store_id
FROM stores
LEFT JOIN orders ON stores.id = orders.store_id;
