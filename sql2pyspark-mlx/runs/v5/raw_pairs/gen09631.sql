WITH Ranked AS (
    SELECT
        id,
        region,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS row_num
    FROM stores
)
SELECT id, region, opened_date
FROM Ranked
WHERE row_num = 1
ORDER BY id;
