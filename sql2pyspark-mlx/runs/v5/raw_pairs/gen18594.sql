SELECT
    order_id,
    QUARTER(shipped_date) AS shipped_quarter,
    MAX(cost) AS max_cost,
    MIN(cost) AS min_cost,
    MIN(shipped_date) AS first_shipped_date
FROM shipments
GROUP BY order_id, shipped_quarter
ORDER BY min_cost DESC;
