SELECT
    channel,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    SUM(CASE WHEN status = 'PENDING' THEN discount ELSE 0 END) AS pending_discount,
    SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count
FROM orders
WHERE order_date > '2024-11-15'
GROUP BY channel
ORDER BY channel;
