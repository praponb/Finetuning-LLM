WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NULL
)
-- QUALIFY-style window filter
SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS rank_in_group
FROM valid
QUALIFY rank_in_group = 1;
