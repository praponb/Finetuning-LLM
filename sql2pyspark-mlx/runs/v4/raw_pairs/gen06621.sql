WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE product_id IN (2, 3)
)
-- Attach stores attributes
SELECT
    src.*,
    d.sqft,
    d.store_name
FROM base src
LEFT JOIN stores d
    ON src.store_id = d.id
