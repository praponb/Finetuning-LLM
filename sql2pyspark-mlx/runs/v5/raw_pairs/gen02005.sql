SELECT
    s.id,
    s.region,
    t.row_count,
    t.max_discount
FROM stores s
INNER JOIN (SELECT store_id, COUNT(*) AS row_count, MAX(discount) AS max_discount
    FROM orders
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY t.row_count DESC, 4 DESC, id
LIMIT 1
