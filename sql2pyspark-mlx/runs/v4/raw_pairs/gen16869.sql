WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(city, '/', region) AS city_region,
        COALESCE(region, 'NONE') AS region_clean
    FROM stores
    WHERE opened_date >= TO_DATE('2023-12-31')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(sqft, 1, 0) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior
    FROM base
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(region, TRUE) OVER (ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        CUME_DIST() OVER (PARTITION BY region, city ORDER BY prev_sqft DESC NULLS LAST) AS prev_sqft_cume_dist,
        AVG(prev_sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_sqft_avg_6
    FROM with_windows
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        PERCENTILE_APPROX(prev_sqft, 0.5) AS prev_sqft_p50,
        COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
        BIT_AND(sqft) AS sqft_bit_and,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', opened_date) ORDER BY COUNT(CASE WHEN city = 'Singapore' THEN 1 END) DESC) AS singapore_count_percent_rank
    FROM enriched
    GROUP BY quarter_start, activity_month
)
SELECT
    quarter_start,
    activity_month,
    city_list,
    prev_sqft_p50,
    sqft_bit_and,
    singapore_count_percent_rank
FROM grouped
ORDER BY prev_sqft_p50 DESC;
