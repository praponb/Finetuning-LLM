WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date NULLS FIRST, id) AS rn
        FROM store_sales
    ) t
    WHERE rn = 1
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.units,
    s.revenue,
    (SELECT MIN(t2.quantity) FROM order_items t2 WHERE t2.product_id = s.product_id) AS min_order_item_quantity
FROM deduped s
ORDER BY min_order_item_quantity NULLS FIRST;
