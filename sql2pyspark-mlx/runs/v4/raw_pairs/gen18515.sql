SELECT
    order_id,
    carrier,
    QUARTER(shipped_date) AS shipped_quarter,
    SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END) AS dhl_count,
    MIN(delivered_date) AS min_delivered_date
FROM shipments
GROUP BY order_id, carrier, shipped_quarter
HAVING MAX(cost) >= 45
ORDER BY dhl_count DESC
