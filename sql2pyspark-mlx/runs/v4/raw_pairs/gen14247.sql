WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(opened_date) AS opened_date_month_end,
        CASE WHEN city IN ('Singapore', 'New York') THEN 'Core' ELSE 'Other' END AS city_group,
        COALESCE(region, 'OTHER') AS region_filled
    FROM stores
    WHERE opened_date >= DATE '2023-05-25'
      AND region IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        LEAD(sqft, 1) OVER (ORDER BY opened_date_month_end, id) AS next_sqft
    FROM filtered
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        city_group,
        region_filled,
        sqft_share,
        next_sqft,
        next_sqft + sqft AS next_sqft_plus_sqft,
        ADD_MONTHS(opened_date_month_end, 3) AS opened_date_month_end_plus_months
    FROM enriched
    WHERE opened_date >= DATE '2024-03-12'
      AND region IS NOT NULL
      AND next_sqft >= 4917
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        city,
        sqft_share,
        next_sqft_plus_sqft
    FROM valid
) src
UNPIVOT (
    reading FOR kpi IN (sqft_share, next_sqft_plus_sqft)
);
