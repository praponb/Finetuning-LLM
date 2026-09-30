SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) method_values
FROM payments
WHERE method IN ('paypal', 'promptpay', 'cash') AND amount IS NOT NULL
GROUP BY order_id
ORDER BY method_values
