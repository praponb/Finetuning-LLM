WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS row_num
    FROM store_sales
)
SELECT id, store_id, units
FROM numbered
WHERE row_num <= 2
ORDER BY store_id DESC;
