WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date NULLS LAST, id) AS latest_rank
        FROM products
    ) t
    WHERE latest_rank = 1
)
SELECT
    id,
    product_name,
    brand,
    cost,
    stock_qty,
    launch_date,
    tags,
    latest_rank
FROM ranked
ORDER BY cost;
