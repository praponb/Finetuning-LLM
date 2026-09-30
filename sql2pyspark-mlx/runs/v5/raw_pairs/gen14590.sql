SELECT s.sqft, s.store_name, t.status_values
FROM stores s
LEFT OUTER JOIN (SELECT store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM orders
    WHERE store_id = 30
    GROUP BY store_id) t ON s.id = t.store_id;
