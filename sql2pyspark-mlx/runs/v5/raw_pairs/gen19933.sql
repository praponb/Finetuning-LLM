WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, SIZE(tags) AS tags_count
    FROM products
    WHERE launch_date >= DATE '2023-09-08'
      AND brand IS NOT NULL
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.stock_qty,
        s.tags,
        s.tags_count,
        top1.inventory_quantity,
        top1.last_restock
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        category,
        tags_count,
        inventory_quantity,
        last_restock
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY stock_qty DESC, id) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock <= s.last_restock
        )
),
deduped_products AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        category,
        tags_count,
        inventory_quantity
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY category ORDER BY tags_count DESC NULLS LAST) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(category)) AS category_title
    FROM deduped_products
    WHERE category IN ('Electronics', 'Home')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT category
    FROM filtered
    EXCEPT ALL
    SELECT category
    FROM products
    WHERE category = 'Electronics'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.category = k.category
)
SELECT
    id,
    tags_count,
    inventory_quantity,
    category_title
FROM verified
ORDER BY id DESC NULLS FIRST, tags_count DESC
