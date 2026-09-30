WITH numbered AS (
    SELECT
        id,
        order_id,
        unit_price,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) rnk
    FROM order_items
    WHERE product_id = 2
)
SELECT
    id,
    order_id,
    unit_price,
    quantity
FROM numbered
WHERE rnk = 1
ORDER BY id, order_id;
