SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rnk
    FROM order_items
    WHERE product_id IS NULL
) AS r
WHERE rnk <= 3
