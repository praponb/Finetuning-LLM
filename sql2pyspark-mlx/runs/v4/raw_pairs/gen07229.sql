SELECT
    order_id,
    SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount,
    MAX(amount) AS max_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM payments
WHERE status = 'REFUNDED' AND order_id = 15
GROUP BY 1;
