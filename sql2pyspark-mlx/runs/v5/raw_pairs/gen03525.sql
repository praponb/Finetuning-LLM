WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(region) AS region_upper
    FROM stores
    WHERE region IS NOT NULL
      AND region = 'North'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(opened_date, 12) AS opened_date_plus_months
    FROM valid
    WHERE opened_date > DATE '2023-06-06'
      AND region NOT IN ('Central', 'East')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
deduped_stores AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        region,
        opened_date,
        sqft,
        opened_date_plus_months,
        row_num
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY row_num DESC) AS rn
        FROM deduped
    ) t
    WHERE rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date_plus_months, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3
    FROM deduped_stores
    WHERE row_num >= 1
)
-- Grouped metrics
SELECT
    CASE WHEN sqft_avg_3 >= 13966 THEN 'HIGH' ELSE 'LOW' END AS sqft_avg_3_tier,
    DATE_TRUNC('QUARTER', opened_date_plus_months) AS quarter_start,
    AVG(sqft_cume_dist) FILTER (WHERE region = 'North') AS north_avg_sqft_cume_dist,
    BIT_AND(sqft) AS sqft_bit_and,
    AVG(sqft) FILTER (WHERE region = 'West') AS west_avg_sqft,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SUM(CASE WHEN region = 'East' THEN sqft_avg_3 ELSE 0 END) AS east_sqft_avg_3
FROM metrics
GROUP BY CASE WHEN sqft_avg_3 >= 13966 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', opened_date_plus_months)
ORDER BY quarter_start DESC;
