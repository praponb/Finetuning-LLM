WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        ADD_MONTHS(opened_date, 12) AS opened_date_plus_months,
        REPLACE(store_name, ' ', '-') AS store_name_slug
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date >= '2024-01-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id ASC) AS row_seq,
        NTILE(4) OVER (PARTITION BY city ORDER BY sqft ASC, id ASC) AS sqft_bucket,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft ASC, id ASC) AS sqft_bucket_2,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY city, region ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        NTILE(4) OVER (PARTITION BY city ORDER BY sqft DESC, id ASC) AS sqft_bucket_3
    FROM base
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date_plus_months
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    ROUND(SUM(sqft_bucket_2) / NULLIF(COUNT(CASE WHEN region = 'Central' THEN 1 END), 0), 2) AS sqft_bucket_2_per_central,
    SUM(row_seq) FILTER (WHERE region = 'Central') AS central_total_row_seq,
    COUNT_IF(region = 'Central') AS central_events
FROM kept
GROUP BY 1
ORDER BY central_events DESC NULLS FIRST
