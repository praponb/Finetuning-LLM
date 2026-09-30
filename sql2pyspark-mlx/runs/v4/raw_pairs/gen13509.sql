WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price * 1.1, 2) AS adjusted_unit_price
    FROM order_items
    WHERE product_id IN (12, 14, 33, 35)
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rank_in_group
    FROM filtered
) t
WHERE rank_in_group = 1;
