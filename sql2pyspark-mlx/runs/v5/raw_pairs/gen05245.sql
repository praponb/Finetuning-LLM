WITH RankedStoreSales AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS rnk
    FROM store_sales
)
SELECT id, store_id, units
FROM RankedStoreSales
WHERE rnk <= 2
