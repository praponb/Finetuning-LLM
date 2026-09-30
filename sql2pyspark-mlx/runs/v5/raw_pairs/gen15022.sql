WITH numbered AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS pos
    FROM order_items
)
SELECT *
FROM numbered
WHERE pos <= 3
ORDER BY quantity DESC
