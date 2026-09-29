WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
      AND cost BETWEEN 250 AND 390
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock <= s.launch_date
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost_filled DESC NULLS LAST, id) AS rank_in_group
    FROM not_flagged
    QUALIFY rank_in_group = 1
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_item_unit_price
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        category,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(CASE WHEN category = 'Home' THEN 1 END) AS home_count,
        COUNT(*) AS num_rows,
        COUNT(*) FILTER (WHERE category = 'Home') AS home_rows,
        SUM(CASE WHEN category = 'Fashion' THEN price ELSE 0 END) AS fashion_price
    FROM with_last
    GROUP BY 1
)
SELECT
    category,
    distinct_ids,
    num_rows,
    home_rows,
    fashion_price,
    CASE WHEN category RLIKE '^(Fashion|Home)$' THEN 'EXTENDED' WHEN category RLIKE '^(Electronics|Sports)$' THEN 'CORE' ELSE 'PRIORITY' END AS category_zone
FROM stats
ORDER BY home_rows DESC NULLS FIRST;
