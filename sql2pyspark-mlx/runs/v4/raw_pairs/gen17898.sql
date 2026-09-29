SELECT
    status,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    COUNT(*) FILTER (WHERE status = 'RETURNED') AS returned_rows,
    MIN(discount) AS min_discount,
    COUNT(*) AS cnt,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS amount_per_store,
    GROUPING(status) AS grouping_id
FROM orders
GROUP BY GROUPING SETS ((status, DATE_TRUNC('MONTH', order_date)), (status))
HAVING returned_rows >= 1
ORDER BY returned_rows NULLS LAST;
