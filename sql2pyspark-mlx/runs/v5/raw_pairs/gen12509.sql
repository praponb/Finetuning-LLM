WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(category)) AS category_title,
        ARRAY_JOIN(tags, ', ') AS tags_text,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE cost <= 590
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.unit_price
    FROM valid s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        tags_text,
        brand_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC NULLS FIRST, id) AS rn
        FROM with_latest
    ) x
    WHERE rn = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id, brand ORDER BY stock_qty, id) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
)
-- Join a dimension and aggregate
SELECT
    s.country,
    a.brand,
    COUNT(*) AS cnt,
    AVG(a.cost) AS avg_cost
FROM ranked a
JOIN suppliers s ON a.supplier_id = s.id
GROUP BY GROUPING SETS ((s.country, a.brand), (s.country))
ORDER BY cnt DESC NULLS LAST
