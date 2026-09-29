WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        city,
        rating,
        TRIM(city) AS clean_city,
        CASE WHEN city IN ('Berlin', 'Singapore') THEN 'Priority' ELSE 'Other' END AS city_group
    FROM suppliers
    WHERE city IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(country) AS clean_country
    FROM base
    WHERE city IS NOT DISTINCT FROM 'Chiang Mai'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        clean_country
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST, id) AS latest_rank
        FROM filtered
    ) ranked
    WHERE latest_rank = 1
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.clean_country,
        top1.product_stock_qty,
        top1.category
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND SIZE(ch.tags) > 1
        )
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY product_stock_qty DESC NULLS LAST, id) AS dedup_rank
    FROM eligible
    QUALIFY dedup_rank = 1
)
-- Grouped metrics
SELECT
    CASE WHEN product_stock_qty >= 318 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_stock_qty_tier,
    category,
    ROUND(SUM(product_stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Electronics' THEN 1 END), 0), 2) AS product_stock_qty_per_electronics,
    MIN(product_stock_qty) AS min_product_stock_qty
FROM deduped
GROUP BY product_stock_qty_tier, category
HAVING ROUND(SUM(product_stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Electronics' THEN 1 END), 0), 2) >= 0;
