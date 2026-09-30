WITH store_sales_by_store AS (
    SELECT store_id, MIN(units) min_units
    FROM store_sales
    WHERE sale_date BETWEEN '2025-06-01' AND '2025-10-01'
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.store_name,
    ssbs.min_units
FROM stores s
INNER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
