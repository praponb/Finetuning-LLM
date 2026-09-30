SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS rn
    FROM shipments
) AS ranked
WHERE rn <= 3
ORDER BY delivered_date DESC;
