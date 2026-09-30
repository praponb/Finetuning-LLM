SELECT
    DATE_FORMAT(paid_at, 'yyyy-MM') paid_month,
    status,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) refunded_count,
    SORT_ARRAY(COLLECT_SET(method)) method_list
FROM payments
WHERE paid_at >= '2023-09-01'
GROUP BY paid_month, status
ORDER BY refunded_count DESC
