WITH valid AS (
    SELECT *, unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
)
SELECT
    id,
    order_id,
    product_id,
    unit_price
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) = 1;
