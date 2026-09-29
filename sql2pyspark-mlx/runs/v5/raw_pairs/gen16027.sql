SELECT
    s.city,
    s.store_name,
    t.unique_channels,
    t.unique_statuss
FROM stores s
LEFT JOIN (SELECT store_id, COUNT(DISTINCT amount) unique_channels, COUNT(DISTINCT discount) unique_statuss
    FROM orders
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.unique_channels > 5
