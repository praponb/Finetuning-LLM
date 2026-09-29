WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        LEAD(sqft, 1, 0) OVER (ORDER BY sqft, id) AS next_sqft,
        ROW_NUMBER() OVER w_recent AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS row_seq,
        LEAD(sqft, 2) OVER (ORDER BY opened_date, id) AS next_sqft_2
    FROM stores
    WINDOW w_recent AS (PARTITION BY city ORDER BY sqft, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LEAST(seq_no, next_sqft_2) AS least_seq_no_next_sqft_2
    FROM with_windows
    WHERE opened_date > DATE '2023-07-26'
      AND region IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        sqft,
        sqft_share,
        next_sqft,
        seq_no,
        next_sqft_2,
        least_seq_no_next_sqft_2,
        CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo,
        COALESCE(region, 'NONE') AS region_clean
    FROM cleaned
    WHERE opened_date >= DATE '2023-12-20'
      AND region IS NOT NULL
      AND LOWER(TRIM(store_name)) = 'airport kiosk'
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN next_sqft_2 >= 15225 THEN 'HIGH' ELSE 'LOW' END AS next_sqft_2_tier,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE region = 'North') AS north_rows
    FROM base
    GROUP BY CASE WHEN next_sqft_2 >= 15225 THEN 'HIGH' ELSE 'LOW' END
)
SELECT next_sqft_2_tier, event_count
FROM stats
ORDER BY event_count, next_sqft_2_tier DESC;
