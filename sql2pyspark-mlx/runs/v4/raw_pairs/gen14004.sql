SELECT stores.id, stores.opened_date
FROM stores
WHERE stores.opened_date < '2023-04-01' AND stores.id IN (SELECT store_id FROM orders WHERE channel IN ('web', 'store'));
