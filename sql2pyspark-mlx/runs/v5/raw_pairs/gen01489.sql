WITH RecentPayments AS (
    SELECT *
    FROM payments
    WHERE order_id IN (6, 22, 27) AND status IN ('FAILED', 'REFUNDED', 'SUCCESS')
),
summary AS (
    SELECT method, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM RecentPayments
    GROUP BY method
)
SELECT method, status_values
FROM summary
ORDER BY status_values DESC, 1;
