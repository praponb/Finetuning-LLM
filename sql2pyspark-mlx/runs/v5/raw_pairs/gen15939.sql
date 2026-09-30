WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        sqft,
        LAG(sqft, 2, 0) OVER (PARTITION BY city, region ORDER BY opened_date, id) AS prev_sqft,
        SUM(sqft) OVER (PARTITION BY city) AS city_sum_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share
    FROM stores
    WHERE sqft BETWEEN 5375 AND 14760
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york,
        CASE WHEN sqft >= 13087 THEN 'High' WHEN sqft >= 2904 THEN 'Medium' ELSE 'Low' END AS sqft_level,
        COALESCE(region, 'UNKNOWN') AS region_filled
    FROM metrics
    WHERE region IS DISTINCT FROM 'North'
      AND region IS NOT NULL
),
metrics_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(sqft_share) OVER (PARTITION BY city ORDER BY sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft_share,
        COUNT(sqft_share) OVER (PARTITION BY city ORDER BY sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft_share,
        MIN(sqft_share) OVER (PARTITION BY city ORDER BY sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft_share
    FROM cleaned
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(is_new_york, 0) AS is_new_york_filled
    FROM metrics_stores
    WHERE region IS DISTINCT FROM 'Central'
      AND city IN ('Bangkok', 'Singapore')
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        sqft,
        prev_sqft,
        city_sum_sqft,
        sqft_share,
        is_new_york,
        sqft_level,
        region_filled,
        running_sum_sqft_share,
        running_count_sqft_share,
        running_min_sqft_share,
        is_new_york_filled,
        ROUND(sqft_share, 2) AS sqft_share_rounded,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM base
    WHERE region IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY running_sum_sqft_share DESC, id) AS latest_rank
    FROM cleaned_stores
    QUALIFY latest_rank = 1
)
SELECT
    id,
    city,
    sqft,
    city_sum_sqft,
    sqft_share,
    sqft_level,
    region_filled,
    is_new_york_filled,
    sqft_share_rounded
FROM deduped
ORDER BY id NULLS LAST, sqft_share_rounded DESC
LIMIT 10;
