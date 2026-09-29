SELECT
    status,
    method,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count
FROM payments
GROUP BY status, method
HAVING paypal_count >= 1
ORDER BY paypal_count;
