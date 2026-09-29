WITH ranked AS (
    SELECT *
    FROM store_sales
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST) = 1
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    revenue
FROM ranked
ORDER BY product_id NULLS LAST, units DESC
