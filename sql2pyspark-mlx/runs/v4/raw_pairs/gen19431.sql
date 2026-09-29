SELECT *, SUBSTRING(carrier, 1, 4) AS carrier_prefix, ROUND(cost * 1.1, 2) AS adjusted_cost
FROM shipments
WHERE YEAR(delivered_date) = 2023 OR cost <= 30
ORDER BY cost DESC, 8
