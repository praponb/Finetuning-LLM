SELECT
    status,
    carrier,
    QUARTER(delivered_date) AS delivered_quarter,
    ROUND((100 * SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_returned,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY status, carrier, delivered_quarter
HAVING pct_returned < 62
ORDER BY stddev_cost DESC;
