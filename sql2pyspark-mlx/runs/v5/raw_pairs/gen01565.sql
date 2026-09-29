WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CAST(quantity AS STRING) AS quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity NULLS LAST) AS latest_rank
FROM cleaned
QUALIFY latest_rank = 1
