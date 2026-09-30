SELECT
    DATE_TRUNC('MONTH', shipped_date) AS shipped_month,
    COUNT(*) AS shipment_count,
    ROUND(AVG(cost), 2) AS avg_cost,
    COUNT(DISTINCT cost) AS unique_carriers
FROM shipments
GROUP BY shipped_month
ORDER BY unique_carriers DESC, shipment_count DESC;
