WITH cleaned AS (
    SELECT *, DATE_SUB(order_date, 23) AS order_date_minus_23d
    FROM orders
    WHERE order_date > DATE '2023-06-06'
      AND amount > 1290
)
SELECT
    status,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS amount_per_completed,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    GROUPING_ID() AS gid
FROM cleaned
GROUP BY GROUPING SETS ((status, DATE_TRUNC('QUARTER', order_date)), (status))
ORDER BY avg_shipping_fee DESC;
