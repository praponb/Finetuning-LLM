WITH deduped AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS FIRST) AS rn
    FROM order_items
    QUALIFY rn = 1
)
SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM deduped;
