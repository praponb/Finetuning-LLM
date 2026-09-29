WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE units > 14
)
SELECT product_id, id
FROM cleaned
EXCEPT
SELECT product_id, id
FROM store_sales
WHERE sale_date >= '2024-08-01'
