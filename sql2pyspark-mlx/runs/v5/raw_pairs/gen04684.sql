WITH src AS (
    SELECT *, COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE store_id IN (4, 5) AND (units IS NOT NULL OR sale_date BETWEEN '2024-04-01' AND '2025-05-01')
),
store_id_summary AS (
    SELECT store_id, MAX(units) AS max_units
    FROM src
    GROUP BY store_id
)
SELECT store_id, max_units
FROM store_id_summary
WHERE max_units < 19
