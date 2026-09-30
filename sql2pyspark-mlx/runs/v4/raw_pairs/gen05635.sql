WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS rank_in_group
        FROM order_items
    ) t
    WHERE rank_in_group = 1
)
SELECT
    id,
    order_id,
    quantity,
    rank_in_group
FROM ranked
ORDER BY quantity ASC NULLS LAST, order_id ASC;
