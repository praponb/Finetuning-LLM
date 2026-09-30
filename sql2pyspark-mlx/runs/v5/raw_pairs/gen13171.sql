SELECT
    s.id,
    s.opened_date,
    s.sqft,
    t.unique_stores,
    t.returned_count
FROM stores s
INNER JOIN (SELECT store_id, COUNT(DISTINCT amount) AS unique_stores, SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count
    FROM orders
    WHERE status != 'PENDING'
    GROUP BY store_id) t ON s.id = t.store_id;
