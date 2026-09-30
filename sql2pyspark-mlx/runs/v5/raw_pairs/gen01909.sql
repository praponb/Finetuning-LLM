WITH cleaned AS (
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        CONCAT_WS('/', status, carrier) AS status_carrier_key,
        CASE WHEN cost >= 104 THEN 'High' WHEN cost >= 19 THEN 'Medium' ELSE 'Low' END AS cost_band
    FROM shipments
    WHERE YEAR(delivered_date) >= 2025
)
SELECT id
FROM cleaned
EXCEPT
SELECT id
FROM shipments
WHERE delivered_date BETWEEN '2024-04-23' AND '2025-10-26';
