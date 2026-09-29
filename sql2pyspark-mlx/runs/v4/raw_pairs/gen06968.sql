WITH numbered AS (
    SELECT
        id,
        product_id,
        quantity,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS rnk
    FROM order_items
)
SELECT *
FROM numbered
WHERE rnk <= 3
ORDER BY product_id, id DESC;
