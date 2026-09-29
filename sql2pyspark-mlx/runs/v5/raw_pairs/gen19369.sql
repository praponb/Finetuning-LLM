SELECT *
FROM (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS pos
    FROM shipments
) AS t
WHERE pos <= 2
ORDER BY delivered_date, pos DESC;
