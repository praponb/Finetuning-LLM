WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LAST_DAY(opened_date) opened_date_month_end,
        WEEKOFYEAR(opened_date) opened_date_weekofyear
    FROM stores
    WHERE opened_date > TO_DATE('2023-03-14')
      AND region IS NOT NULL
      AND city = 'Tokyo'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_month_end,
        opened_date_weekofyear,
        NTILE(4) OVER (ORDER BY sqft DESC, id) sqft_bucket,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_date_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_sqft
    FROM filtered
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date_month_end DESC, id) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
ranked_stores AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY latest_rank DESC) = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked_stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units != 24
        )
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.inventory_last_restock
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date_month_end
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.units
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('WEEK', opened_date_month_end) AS week_start,
    SUM(sqft_bucket) AS total_sqft_bucket,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(CASE WHEN region = 'West' THEN 1 END) AS west_count,
    ROUND(AVG(sqft), 2) AS avg_sqft_2,
    GROUPING(DATE_TRUNC('QUARTER', opened_date)) AS grouping_level
FROM with_latest
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', opened_date), DATE_TRUNC('WEEK', opened_date_month_end)), (DATE_TRUNC('QUARTER', opened_date)))
ORDER BY avg_sqft;
