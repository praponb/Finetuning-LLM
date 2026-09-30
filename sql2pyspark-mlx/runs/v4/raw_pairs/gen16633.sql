WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        units,
        revenue,
        DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
    FROM store_sales
    WHERE sale_date >= DATE '2024-04-20'
)
SELECT store_id, id
FROM cleaned
EXCEPT ALL
SELECT store_id, id
FROM store_sales
WHERE units = 12;
