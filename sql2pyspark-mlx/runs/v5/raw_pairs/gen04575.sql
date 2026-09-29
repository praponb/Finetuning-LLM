SELECT
    QUARTER(shipped_date) AS shipped_quarter,
    AVG(cost) AS avg_cost,
    MAX(delivered_date) AS latest_delivered_date,
    MIN(delivered_date) AS min_delivered_date
FROM shipments
GROUP BY 1
ORDER BY 1;
