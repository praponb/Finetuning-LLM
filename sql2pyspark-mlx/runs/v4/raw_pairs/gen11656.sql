WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS pos
    FROM store_sales
)
SELECT id, store_id, units
FROM ranked
WHERE pos <= 5
ORDER BY 3 ASC;
