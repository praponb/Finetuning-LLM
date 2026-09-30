WITH Base AS (
    SELECT *
    FROM store_sales
    WHERE revenue IS NULL
),
summary AS (
    SELECT product_id, MAX(sale_date) AS latest_sale_date
    FROM Base
    GROUP BY product_id
)
SELECT *
FROM summary
ORDER BY product_id DESC
LIMIT 1;
