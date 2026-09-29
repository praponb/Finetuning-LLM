WITH store_sales_by_store AS (
    SELECT store_id, SUM(units) AS total_units
    FROM store_sales
    GROUP BY store_id
)
SELECT s.region, s.sqft, ssbs.total_units
FROM stores s
LEFT JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
WHERE ssbs.total_units > 200
ORDER BY total_units, s.region DESC;
