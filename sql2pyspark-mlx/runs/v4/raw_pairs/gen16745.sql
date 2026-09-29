WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price * 1.15, 2) AS adjusted_unit_price
    FROM order_items
    WHERE product_id IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY unit_price NULLS LAST) AS rank_in_group
FROM filtered
QUALIFY rank_in_group = 1;
