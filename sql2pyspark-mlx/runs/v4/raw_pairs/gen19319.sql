WITH store_summary AS (
    SELECT store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values, MAX(order_date) AS last_order_date
    FROM orders
    GROUP BY store_id
)
SELECT
    stores.opened_date,
    stores.region,
    store_summary.channel_values,
    store_summary.last_order_date
FROM stores
LEFT OUTER JOIN store_summary ON stores.id = store_summary.store_id
ORDER BY stores.region DESC, opened_date;
