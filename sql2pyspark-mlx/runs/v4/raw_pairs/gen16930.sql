SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) rn
    FROM shipments
) ranked
WHERE rn <= 5
