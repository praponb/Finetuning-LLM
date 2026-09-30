SELECT *
FROM stores
WHERE LENGTH(store_name) > 11 AND region = 'East'
