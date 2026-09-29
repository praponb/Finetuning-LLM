SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM subscriptions
WHERE start_date <= '2024-12-21'
GROUP BY status;
