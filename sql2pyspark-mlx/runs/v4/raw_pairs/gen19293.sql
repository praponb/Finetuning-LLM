WITH order_totals AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, COUNT(DISTINCT amount) AS unique_statuss
    FROM payments
    WHERE TO_DATE(paid_at) < '2023-08-01'
    GROUP BY order_id
)
SELECT
    o.id,
    o.discount,
    o.store_id,
    ot.status_values,
    ot.unique_statuss
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id;
