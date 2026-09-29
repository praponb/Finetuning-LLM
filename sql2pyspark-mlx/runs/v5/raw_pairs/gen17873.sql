WITH ranked_store_sales AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS row_num
    FROM store_sales
    WHERE units > 32
)
SELECT
    id,
    store_id,
    sale_date,
    units
FROM ranked_store_sales
WHERE row_num <= 3
ORDER BY sale_date;
