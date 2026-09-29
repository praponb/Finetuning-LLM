WITH StoreStats AS (
    SELECT store_id, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, MAX(order_date) AS last_order_date
    FROM orders
    WHERE customer_id IN (16, 32, 35, 38)
    GROUP BY store_id
)
SELECT
    s.region,
    s.sqft,
    S.channel_list,
    S.last_order_date
FROM stores s
INNER JOIN StoreStats S ON s.id = S.store_id
