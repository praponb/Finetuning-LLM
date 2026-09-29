WITH ranked AS (
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS LAST, id) = 1
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft
FROM ranked
ORDER BY region NULLS LAST;
