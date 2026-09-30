SELECT CASE WHEN amount >= 1440 THEN 'high' WHEN amount >= 800 THEN 'mid' ELSE 'low' END AS amount_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM payments
WHERE status = 'SUCCESS'
GROUP BY amount_bucket;
