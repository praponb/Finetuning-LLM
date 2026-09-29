WITH base AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label
    FROM store_sales
    WHERE sale_date > DATE '2023-04-27'
)
-- Attach stores attributes
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.sale_date_label,
    p2.store_name
FROM base s
INNER JOIN stores p2
    ON s.store_id = p2.id
WHERE p2.sqft < 8850;
