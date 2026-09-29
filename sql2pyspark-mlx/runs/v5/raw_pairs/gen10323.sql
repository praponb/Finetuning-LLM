WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY revenue NULLS FIRST) AS latest_rank
    FROM store_sales
    QUALIFY latest_rank = 1
)
SELECT
    id,
    store_id,
    product_id,
    units,
    revenue
FROM latest
ORDER BY store_id NULLS FIRST, units;
