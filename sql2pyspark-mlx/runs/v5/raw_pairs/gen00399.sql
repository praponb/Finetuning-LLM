WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(sqft) sqft_whole,
        SUBSTRING(city, 1, 2) city_prefix
    FROM stores
    WHERE city = 'Bangkok'
      AND opened_date >= TO_DATE('2023-04-24')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS FIRST, id ASC) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(sqft_whole, 2) OVER (ORDER BY sqft_whole ASC, id ASC) prev_sqft_whole,
        NTILE(10) OVER (ORDER BY sqft_whole DESC, id ASC) sqft_whole_bucket,
        AVG(sqft) OVER (PARTITION BY region) region_avg_sqft
    FROM latest
),
latest_stores AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft_whole DESC NULLS LAST, id ASC) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        sqft_whole,
        latest_rank,
        prev_sqft_whole,
        sqft_whole_bucket,
        region_avg_sqft,
        rn,
        ROW_NUMBER() OVER (ORDER BY region_avg_sqft DESC, id ASC) row_seq,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY latest_rank DESC, id ASC) row_seq_2
    FROM latest_stores
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        row_seq_2
    FROM with_windows
) src
PIVOT (
    AVG(row_seq_2)
    FOR region IN ('North' AS north, 'West' AS west)
)
