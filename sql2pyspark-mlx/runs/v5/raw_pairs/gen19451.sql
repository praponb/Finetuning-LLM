WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (1, 4, 20)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT id, product_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity NULLS FIRST, id) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        p.category
    FROM deduped e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.brand = 'Initech'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(category, 'N/A') AS category_filled,
        COALESCE(category, 'UNKNOWN') AS category_clean
    FROM joined
    WHERE category IS NOT NULL
),
valid_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        category,
        category_filled,
        category_clean,
        SUBSTRING(category, 1, 4) AS category_prefix,
        COALESCE(category, 'NONE') AS resolved_category
    FROM valid
    WHERE category IN ('Fashion', 'Books')
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT id) AS distinct_ids
FROM valid_order_items
GROUP BY 1
HAVING distinct_ids > 1;
