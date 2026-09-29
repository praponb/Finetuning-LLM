WITH recent_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE revenue < 3400
),
Summary AS (
    SELECT
        store_id,
        product_id,
        COUNT(units) AS units_count,
        COUNT(DISTINCT revenue) AS unique_stores
    FROM recent_store_sales
    GROUP BY store_id, product_id
)
SELECT *
FROM Summary
WHERE units_count < 9
ORDER BY unique_stores;
