WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        QUARTER(opened_date) AS opened_date_quarter,
        CASE WHEN region IN ('North', 'South') THEN 'Group A' ELSE 'Other' END AS region_group,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-04-25'
      AND region IN ('East', 'West')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        opened_date_quarter,
        region_group
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft,
        region_group,
        NTILE(5) OVER (PARTITION BY region ORDER BY opened_date_quarter, id) AS opened_date_quarter_bucket,
        LAG(opened_date_quarter, 2) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_opened_date_quarter
    FROM latest
    WHERE sqft BETWEEN 11679 AND 12848
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (ORDER BY opened_date_quarter_bucket DESC, id) AS opened_date_quarter_bucket_bucket,
        LAG(opened_date_quarter_bucket, 1) OVER (ORDER BY sqft, id) AS prev_opened_date_quarter_bucket,
        ROW_NUMBER() OVER (ORDER BY opened_date_quarter_bucket DESC, id) AS event_seq
    FROM with_windows
)
SELECT
    id,
    sqft,
    opened_date_quarter_bucket,
    prev_opened_date_quarter_bucket,
    event_seq
FROM with_windows_stores
ORDER BY id DESC
LIMIT 5
