SELECT *
FROM (
    SELECT status, MIN(delivered_date) AS first_delivered_date, COUNT(*) AS row_count
    FROM shipments
    WHERE order_id = 20
    GROUP BY status
) AS agg
WHERE row_count >= 8
ORDER BY 1, 3 DESC;
