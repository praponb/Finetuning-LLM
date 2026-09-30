WITH base AS (
    SELECT *, quantity + unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    order_id,
    unit_price,
    line_total
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) AS latest_rank
    FROM base
) ranked
WHERE latest_rank = 1
