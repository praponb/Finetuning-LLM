WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS row_num
    FROM order_items
)
SELECT
    id,
    product_id,
    unit_price,
    quantity
FROM numbered
WHERE row_num <= 1
ORDER BY unit_price DESC, id DESC
