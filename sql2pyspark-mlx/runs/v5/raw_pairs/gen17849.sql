WITH ranked AS (
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS LAST, id) = 1
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date
FROM ranked
ORDER BY city DESC;
