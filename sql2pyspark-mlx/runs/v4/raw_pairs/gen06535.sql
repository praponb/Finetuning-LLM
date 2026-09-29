WITH latest AS (
    SELECT *
    FROM suppliers
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY rating DESC) = 1
)
SELECT
    id,
    supplier_name,
    country,
    rating
FROM latest
ORDER BY rating
