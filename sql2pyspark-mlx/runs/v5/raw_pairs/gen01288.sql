WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (10, 15, 28)
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END AS is_grocery,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM eligible
    WHERE ARRAY_CONTAINS(tags, 'bestseller')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, is_grocery * cost AS is_grocery_times_cost
    FROM base
    WHERE brand IS NOT NULL
      AND is_grocery > 56
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity BETWEEN 3 AND 4
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(cost * 1.15, 2) AS cost_scaled
    FROM not_flagged
    WHERE launch_date >= DATE '2023-05-07'
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    launch_date,
    is_grocery
FROM filtered
ORDER BY cost NULLS LAST
