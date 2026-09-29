WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(revenue, 0) AS revenue_rounded
    FROM store_sales
    WHERE product_id IN (5, 8)
),
joined AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.brand,
        p.category
    FROM valid a
    LEFT JOIN products p
        ON a.product_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, brand ORDER BY revenue_rounded NULLS LAST, id) AS dedup_rank
        FROM joined
    ) t
    WHERE dedup_rank = 1
),
latest_store_sales AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_rounded DESC, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', category, brand) AS category_brand_key,
        ROUND(dedup_rank * 1.07, 2) AS dedup_rank_scaled
    FROM latest_store_sales
    WHERE sale_date > DATE '2024-03-02'
      AND brand IS NOT NULL
      AND units IS NOT NULL
)
SELECT
    id,
    sale_date,
    units,
    revenue,
    revenue_rounded,
    brand,
    dedup_rank,
    category_brand_key,
    dedup_rank_scaled
FROM base
