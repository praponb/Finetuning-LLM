WITH store_stats AS (
    SELECT store_id, SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) mobile_count
    FROM orders
    GROUP BY store_id
)
SELECT s.region, ss.mobile_count
FROM stores s
LEFT OUTER JOIN store_stats ss ON s.id = ss.store_id;
