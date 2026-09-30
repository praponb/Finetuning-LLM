SELECT s.id, s.sqft, t.channel_list
FROM stores s
INNER JOIN (SELECT store_id, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
    FROM orders
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY sqft DESC;
