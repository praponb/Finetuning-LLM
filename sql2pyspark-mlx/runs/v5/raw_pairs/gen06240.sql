SELECT s.id, s.region, t.avg_revenue
FROM stores s
LEFT OUTER JOIN (SELECT store_id, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.avg_revenue > 2600;
