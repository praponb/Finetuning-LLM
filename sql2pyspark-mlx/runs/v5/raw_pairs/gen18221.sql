SELECT
    MONTH(o.order_date) AS order_month,
    o.status,
    ROUND((100 * SUM(CASE WHEN s.carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_dhl,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY MONTH(o.order_date), o.status
