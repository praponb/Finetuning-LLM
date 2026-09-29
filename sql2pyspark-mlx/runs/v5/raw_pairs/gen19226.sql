SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS row_num
    FROM shipments
) AS ranked
WHERE row_num <= 1
ORDER BY 3 DESC, 7;
