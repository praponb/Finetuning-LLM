WITH valid_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE sale_date BETWEEN '2025-02-08' AND '2025-07-01' OR revenue >= 1300
),
store_id_summary AS (
    SELECT store_id, COUNT(*) AS cnt, MAX(units) - MIN(units) AS units_range
    FROM valid_store_sales
    GROUP BY store_id
)
SELECT *
FROM store_id_summary;
