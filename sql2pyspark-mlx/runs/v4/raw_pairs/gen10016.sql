WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE opened_date >= DATE '2023-04-25'
      AND region IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_percent_rank,
        LAG(days_since_opened_date, 1) OVER (PARTITION BY city, region ORDER BY days_since_opened_date ASC, id ASC) AS prev_days_since_opened_date
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY region) AS region_max_sqft,
        NTILE(3) OVER w_running AS days_since_opened_date_bucket,
        LEAD(sqft_percent_rank, 1) OVER w_ordered AS next_sqft_percent_rank
    FROM metrics
    WINDOW w_running AS (PARTITION BY city ORDER BY days_since_opened_date DESC, id ASC), w_ordered AS (PARTITION BY city ORDER BY prev_days_since_opened_date ASC, id ASC)
)
-- Running and moving aggregates
SELECT
    *,
    LAG(days_since_opened_date_bucket, 1) OVER (PARTITION BY city ORDER BY prev_days_since_opened_date ASC, id ASC) AS prev_days_since_opened_date_bucket,
    RANK() OVER (PARTITION BY region, city ORDER BY region_max_sqft DESC) AS region_max_sqft_rank
FROM enriched
