SELECT
    MONTH(delivered_date) AS delivered_month,
    MAX(cost) AS max_cost,
    COUNT(DISTINCT cost) AS unique_statuss,
    COUNT(CASE WHEN status = 'LOST' THEN 1 END) AS lost_count
FROM shipments
GROUP BY 1
ORDER BY unique_statuss DESC;
