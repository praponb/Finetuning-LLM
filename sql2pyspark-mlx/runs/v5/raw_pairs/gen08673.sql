WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS pos
    FROM inventory
)
SELECT *
FROM numbered
WHERE pos = 1;
