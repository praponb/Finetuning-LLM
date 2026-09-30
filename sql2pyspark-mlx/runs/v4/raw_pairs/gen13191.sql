WITH cleaned AS (
    SELECT *, GREATEST(quantity, unit_price) greatest_quantity_unit_price
    FROM order_items
    WHERE product_id = 9
)
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY greatest_quantity_unit_price NULLS LAST) AS rank_in_group
    FROM cleaned
) x
WHERE rank_in_group = 1
