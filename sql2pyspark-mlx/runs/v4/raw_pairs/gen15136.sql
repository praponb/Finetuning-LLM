WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(unit_price * 0.9, 2) adjusted_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    adjusted_unit_price,
    FIRST_VALUE(unit_price, TRUE) OVER w_grp first_known_unit_price,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY adjusted_unit_price, id) seq_no
FROM cleaned
WINDOW w_grp AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
