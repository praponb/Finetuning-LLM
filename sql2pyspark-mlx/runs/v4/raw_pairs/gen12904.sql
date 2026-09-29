WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE quantity != 6
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_tier,
        COALESCE(greatest_quantity_unit_price, 0) AS greatest_quantity_unit_price_filled
    FROM valid
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY greatest_quantity_unit_price_filled DESC NULLS FIRST) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        greatest_quantity_unit_price,
        quantity_tier,
        greatest_quantity_unit_price_filled,
        rank_in_group,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity
    FROM latest
)
-- Grouped metrics
SELECT
    CASE WHEN greatest_quantity_unit_price >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_quantity_unit_price_tier,
    product_id,
    CORR(unit_price, quantity) AS corr_unit_price_quantity,
    MAX(unit_price_share) AS max_unit_price_share
FROM metrics
GROUP BY CUBE (CASE WHEN greatest_quantity_unit_price >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id)
HAVING max_unit_price_share < 0
