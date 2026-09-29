SELECT
    carrier,
    SUM(CASE WHEN carrier = 'FedEx' THEN cost ELSE 0 END) AS fedex_cost,
    SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) AS flash_cost,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY 1
