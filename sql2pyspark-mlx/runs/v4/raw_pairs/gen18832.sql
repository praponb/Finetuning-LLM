WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(sqft, 0) sqft_filled,
        COALESCE(region, 'DIRECT') resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-09-13'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(store_name) store_name_lower,
        COALESCE(region, 'N/A') region_filled
    FROM base
    WHERE opened_date >= DATE '2023-12-09'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_filled,
        resolved_region,
        store_name_lower,
        region_filled,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST) sqft_cume_dist,
        SUM(sqft_filled) OVER w_grp running_sqft_filled,
        AVG(sqft_filled) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) sqft_filled_avg_2_prior
    FROM filtered
    WINDOW w_grp AS (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN sqft >= 17152 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    COUNT(*) AS num_rows
FROM scored
GROUP BY CUBE (CASE WHEN sqft >= 17152 THEN 'LARGE' ELSE 'SMALL' END)
HAVING num_rows > 1
