WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS LAST, id) AS rank_in_group
        FROM order_items
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    s.id,
    s.order_id,
    s.quantity,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS sum_order_item_quantity
FROM latest s
ORDER BY id DESC NULLS LAST, sum_order_item_quantity;
