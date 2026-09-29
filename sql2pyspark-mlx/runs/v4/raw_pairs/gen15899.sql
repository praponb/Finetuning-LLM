WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_ADD(opened_date, 27) AS opened_date_plus_27d
    FROM stores
    WHERE opened_date > TO_DATE('2023-05-05')
      AND region NOT IN ('East', 'North', 'West')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        opened_date_plus_27d,
        UPPER(store_name) AS store_name_upper
    FROM cleaned
    WHERE sqft = 10864
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        LEAD(sqft, 2, 0) OVER (ORDER BY sqft, id) AS next_sqft,
        AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
        LEAD(sqft, 1) OVER (PARTITION BY region ORDER BY sqft, id) AS next_sqft_2,
        RANK() OVER (PARTITION BY region ORDER BY sqft NULLS FIRST) AS sqft_rank
    FROM valid
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(next_sqft, 1) OVER (PARTITION BY city ORDER BY sqft, id) AS next_next_sqft,
        ROUND(next_sqft_2 / SUM(next_sqft_2) OVER (PARTITION BY city), 4) AS next_sqft_2_share,
        LAST_VALUE(next_sqft) IGNORE NULLS OVER w_hist AS last_known_next_sqft
    FROM metrics
    WINDOW w_hist AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Running and moving aggregates
SELECT
    *,
    AVG(next_sqft_2) OVER (PARTITION BY region, city ORDER BY opened_date_plus_27d, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS next_sqft_2_avg_3_prior,
    SUM(next_sqft) OVER (ORDER BY sqft_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_sqft,
    ROUND(next_next_sqft / SUM(next_next_sqft) OVER (PARTITION BY city, region), 4) AS next_next_sqft_share
FROM with_windows
