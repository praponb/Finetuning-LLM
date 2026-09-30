WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        UPPER(city) AS city_upper,
        TRIM(city) AS clean_city
    FROM stores
    WHERE region IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date DESC, id) AS rank_in_group
        FROM valid
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        NTILE(5) OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS sqft_bucket,
        CUME_DIST() OVER (PARTITION BY city, region ORDER BY sqft DESC) AS sqft_cume_dist
    FROM latest
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        city_upper,
        clean_city,
        rank_in_group,
        sqft_share,
        sqft_bucket,
        sqft_cume_dist,
        CASE WHEN city IN ('New York', 'London') THEN 'Group A' ELSE 'Other' END AS city_group,
        COALESCE(region, 'NONE') AS region_filled
    FROM enriched
    WHERE opened_date > DATE '2024-03-25'
      AND region IS NOT NULL
),
latest_stores AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_bucket DESC NULLS LAST, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        city_upper,
        clean_city,
        rank_in_group,
        sqft_share,
        sqft_bucket,
        sqft_cume_dist,
        city_group,
        region_filled,
        rn,
        LEAD(sqft_cume_dist, 1) OVER (PARTITION BY region ORDER BY sqft_share, id) AS next_sqft_cume_dist,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft_share) AS sqft_share_cume_dist
    FROM latest_stores
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        region,
        next_sqft_cume_dist,
        sqft_share,
        sqft_share_cume_dist
    FROM scored
) src
UNPIVOT (
    reading FOR metric_name IN (next_sqft_cume_dist AS NEXT_SQFT_CUME_DIST, sqft_share AS SQFT_SHARE, sqft_share_cume_dist AS SQFT_SHARE_CUME_DIST)
)
