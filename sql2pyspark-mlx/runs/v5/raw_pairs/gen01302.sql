WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ARRAY_JOIN(tags, '|') AS tags_text,
        ARRAY_JOIN(tags, ', ') AS tags_text_2
    FROM products
    WHERE brand IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        tags_text
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, category ORDER BY launch_date NULLS LAST, id) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_name,
        brand,
        launch_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC NULLS FIRST, id) AS rank_in_group
        FROM latest
    ) ranked
    WHERE rank_in_group = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC NULLS LAST, id) = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM ranked
    EXCEPT
    SELECT id
    FROM products
    WHERE category = 'Fashion'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN matched_keys k ON s.id = k.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id = 27
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date
    FROM eligible
    WHERE launch_date >= DATE '2023-05-30'
      AND launch_date < '2024-09-06'
)
SELECT
    product_name,
    brand,
    launch_date,
    CASE WHEN brand RLIKE '^(Initech|Acme)$' THEN 'CORE' WHEN brand RLIKE '^(Globex|Umbrella)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS brand_zone
FROM base
ORDER BY launch_date DESC NULLS FIRST
