SELECT
    MONTH(delivered_date) AS delivered_month,
    DATE_TRUNC('MONTH', shipped_date) AS shipped_month,
    MAX(cost) AS max_cost,
    SUM(CASE WHEN status = 'LOST' THEN cost ELSE 0 END) AS lost_cost
FROM shipments
WHERE status = 'RETURNED'
GROUP BY MONTH(delivered_date), DATE_TRUNC('MONTH', shipped_date);
