SELECT
    id,
    carrier,
    cost,
    status,
    AVG(cost) OVER (PARTITION BY carrier) AS carrier_avg_cost,
    ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY cost DESC, id) AS row_num
FROM shipments
WHERE YEAR(shipped_date) = 2023
