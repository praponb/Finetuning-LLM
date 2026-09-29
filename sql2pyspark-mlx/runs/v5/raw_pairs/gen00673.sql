SELECT
    s.id,
    s.store_name,
    s.sqft,
    t.avg_revenue
FROM stores s
JOIN (SELECT store_id, AVG(revenue) avg_revenue
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY store_name ASC, s.sqft DESC;
