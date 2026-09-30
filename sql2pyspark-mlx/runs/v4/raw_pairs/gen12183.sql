SELECT
    QUARTER(delivered_date) AS delivered_quarter,
    SUM(CASE WHEN carrier = 'Kerry' THEN cost ELSE 0 END) AS kerry_cost,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
    COUNT(cost) AS cost_count
FROM shipments
GROUP BY QUARTER(delivered_date)
ORDER BY cost_count, fedex_count
