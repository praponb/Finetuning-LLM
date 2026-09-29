WITH Base AS (
    SELECT *
    FROM order_items
    WHERE order_id = 10 AND quantity BETWEEN 7 AND 8
),
product_id_summary AS (
    SELECT
        product_id,
        order_id,
        MAX(quantity) - MIN(quantity) AS quantity_range,
        COUNT(*) AS cnt
    FROM Base
    GROUP BY product_id, order_id
)
SELECT *
FROM product_id_summary
ORDER BY cnt DESC
