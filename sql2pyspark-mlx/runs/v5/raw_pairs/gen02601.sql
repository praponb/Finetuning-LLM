WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_SUB(opened_date, 79) AS opened_date_minus_79d,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE region IS NOT DISTINCT FROM 'Central'
      AND region IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units = 13
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date_minus_79d,
        region_clean,
        LAG(sqft, 1) OVER (ORDER BY opened_date, id) AS prev_sqft,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) AS sqft_share,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date_minus_79d, id) AS seq_no_2
    FROM not_flagged
    WINDOW w_ordered AS (PARTITION BY region ORDER BY sqft DESC, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        region_clean,
        prev_sqft,
        seq_no,
        sqft_share,
        seq_no_2,
        sqft_share * prev_sqft AS sqft_share_times_prev_sqft,
        COALESCE(region, 'N/A') AS resolved_region
    FROM with_windows
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'South'
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_sqft >= 13338 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_sqft_tier,
    region,
    COUNT(*) AS num_rows,
    ROUND(AVG(seq_no_2), 2) AS avg_seq_no_2,
    AVG(seq_no_2) FILTER (WHERE region = 'North') AS north_avg_seq_no_2,
    GROUPING_ID() AS grouping_id
FROM valid
GROUP BY CUBE (CASE WHEN prev_sqft >= 13338 THEN 'PREMIUM' ELSE 'STANDARD' END, region)
HAVING num_rows > 1
