WITH store_sales_by_store AS (
    SELECT store_id, SUM(units) AS total_units, COUNT(*) AS total_count
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.city,
    ssbs.total_units,
    ssbs.total_count
FROM stores s
INNER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
WHERE ssbs.total_count > 10
ORDER BY ssbs.total_units DESC
