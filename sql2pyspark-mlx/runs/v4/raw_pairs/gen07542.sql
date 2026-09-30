SELECT CASE WHEN channel IN ('store', 'partner') THEN 'tier_1' ELSE 'other' END AS channel_group, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
WHERE customer_id IN (8, 11, 16, 40)
GROUP BY 1
