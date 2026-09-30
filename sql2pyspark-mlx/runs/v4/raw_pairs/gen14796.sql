WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_date_label
    FROM store_sales
    WHERE sale_date > TO_DATE('2024-03-21')
)
-- Attach products attributes
SELECT
    e.*,
    b.price,
    b.launch_date
FROM base e
LEFT JOIN products b
    ON e.product_id = b.id
