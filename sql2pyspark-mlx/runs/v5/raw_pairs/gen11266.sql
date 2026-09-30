SELECT
    s.id,
    s.store_name,
    s.region,
    s.city
FROM stores s
WHERE s.id IN (SELECT store_id FROM orders WHERE discount <= 0.31)
ORDER BY s.city, id DESC
LIMIT 5;
