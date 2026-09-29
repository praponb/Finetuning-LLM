SELECT
    MONTH(delivered_date) AS delivered_month,
    status,
    COUNT(cost) AS cost_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    AVG(cost) AS avg_cost
FROM shipments
WHERE status = 'RETURNED' AND order_id IN (5, 25, 26) AND delivered_date >= '2024-06-01'
GROUP BY MONTH(delivered_date), status;
