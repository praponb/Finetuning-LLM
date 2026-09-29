WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(product_name, 'Unknown') AS product_name_filled,
        COALESCE(brand, 'DIRECT') AS resolved_brand
    FROM products
    WHERE brand IS NOT NULL
      AND supplier_id IN (2, 8, 30)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity > 5
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT brand, id
    FROM not_flagged
    EXCEPT ALL
    SELECT brand, id
    FROM products
    WHERE supplier_id IN (22, 32, 36)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    JOIN matched_keys k ON s.brand = k.brand AND s.id = k.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, category
    FROM kept_rows
    EXCEPT
    SELECT id, category
    FROM products
    WHERE SIZE(tags) >= 2
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN kept_keys k ON s.id = k.id AND s.category = k.category
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (2, 18, 20)
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        category,
        cost,
        price
    FROM kept
) src
UNPIVOT (
    metric_value FOR metric_name IN (cost, price)
);
