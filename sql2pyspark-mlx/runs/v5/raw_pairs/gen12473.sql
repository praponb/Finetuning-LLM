WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DAY(opened_date) AS opened_date_day
    FROM stores
    WHERE region IS NOT NULL
      AND sqft > 9261
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY city, region ORDER BY opened_date_day NULLS LAST) AS opened_date_day_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share,
        PERCENT_RANK() OVER (ORDER BY opened_date_day DESC NULLS LAST) AS opened_date_day_percent_rank
    FROM valid
    WHERE sqft BETWEEN 5584 AND 13354
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(sqft AS STRING) AS sqft_num
    FROM with_windows
    WHERE opened_date > DATE '2023-02-14'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date_day_rank DESC, id) = 1
),
with_windows_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(opened_date_day_percent_rank) OVER w_part AS running_count_opened_date_day_percent_rank,
        MIN(opened_date_day_percent_rank) OVER w_part AS running_min_opened_date_day_percent_rank,
        AVG(opened_date_day_percent_rank) OVER w_part AS running_avg_opened_date_day_percent_rank
    FROM latest
    WINDOW w_part AS (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    opened_date,
    opened_date_day,
    opened_date_day_rank,
    sqft_share,
    opened_date_day_percent_rank,
    sqft_num,
    running_avg_opened_date_day_percent_rank
FROM with_windows_stores
ORDER BY opened_date_day_percent_rank NULLS FIRST
