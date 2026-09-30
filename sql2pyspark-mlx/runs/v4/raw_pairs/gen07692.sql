SELECT *
FROM (
    SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, COUNT(amount) AS amount_count
    FROM payments
    WHERE status = 'FAILED'
    GROUP BY status
) AS sub
WHERE amount_count >= 8
ORDER BY method_values
