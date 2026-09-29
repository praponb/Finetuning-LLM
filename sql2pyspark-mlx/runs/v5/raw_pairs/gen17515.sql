WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        region,
        opened_date,
        sqft,
        CONCAT(region, ' - ', store_name) AS region_store_name,
        COALESCE(region, 'NONE') AS region_clean
    FROM stores
    WHERE opened_date >= TO_DATE('2023-03-09')
      AND region IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN region IN ('East', 'North') THEN 'Priority' ELSE 'Other' END AS region_group,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM base
    WHERE opened_date > TO_DATE('2023-05-03')
),
base_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(opened_date) AS opened_date_month_end,
        CAST(sqft AS STRING) AS sqft_str
    FROM filtered
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'East'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.units <> 20
        )
),
not_flagged_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity <= 113
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_month_end, id) AS seq_no,
        LEAD(sqft, 1) OVER (ORDER BY opened_date_month_end, id) AS next_sqft
    FROM not_flagged_stores
    WHERE YEAR(opened_date_month_end) = 2023
),
filtered_stores AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        region,
        sqft,
        region_clean,
        region_group,
        resolved_region,
        opened_date_month_end,
        sqft_str,
        sqft_avg_3,
        seq_no,
        FLOOR(seq_no) AS seq_no_whole,
        GREATEST(sqft_avg_3, next_sqft) AS greatest_sqft_avg_3_next_sqft
    FROM scored
    WHERE opened_date > DATE '2023-04-10'
      AND region IS NOT NULL
)
-- Grouped metrics
SELECT
    seq_no,
    CASE WHEN greatest_sqft_avg_3_next_sqft >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_sqft_avg_3_next_sqft_tier,
    AVG(greatest_sqft_avg_3_next_sqft) AS avg_greatest_sqft_avg_3_next_sqft,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    COUNT(*) FILTER (WHERE region = 'North') AS north_rows
FROM filtered_stores
GROUP BY seq_no, CASE WHEN greatest_sqft_avg_3_next_sqft >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY seq_no DESC NULLS FIRST;
