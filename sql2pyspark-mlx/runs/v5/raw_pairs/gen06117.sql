WITH numbered AS (
    SELECT
        id,
        store_id,
        units,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS pos
    FROM store_sales
    WHERE store_id IN (1, 4, 6)
)
SELECT *
FROM numbered
WHERE pos = 1
ORDER BY pos;
