WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        quantity,
        DAY(last_restock) last_restock_day
    FROM inventory
    WHERE quantity > 72
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        last_restock_day
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) = 1
),
filtered_inventory AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(last_restock_day * 1.1, 2) last_restock_day_scaled
    FROM ranked
    WHERE last_restock_day != 76
),
joined AS (
    -- Attach products attributes
    SELECT
        e.*,
        p.cost,
        p.category
    FROM filtered_inventory e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.category IN ('Home', 'Fashion')
)
SELECT
    s.id,
    s.product_id,
    s.last_restock_day,
    s.last_restock_day_scaled,
    (SELECT SUM(x.unit_price) FROM order_items x WHERE x.product_id = s.product_id) AS sum_order_item_unit_price
FROM joined s
ORDER BY product_id, id
