WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS rank_in_group
    FROM order_items
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    product_id,
    unit_price,
    rank_in_group
FROM ranked
ORDER BY product_id DESC
