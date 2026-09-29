WITH numbered AS (
    SELECT
        id,
        product_id,
        unit_price,
        RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS row_num
    FROM order_items
    WHERE order_id = 13
)
SELECT *
FROM numbered
WHERE row_num <= 2
