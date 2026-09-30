WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        rating,
        CONCAT(country, ' - ', city) AS country_city,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_avg_6_prior,
        SUM(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        NTILE(4) OVER (ORDER BY rating DESC, id ASC) AS rating_bucket,
        SUM(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_2
    FROM base
    WHERE supplier_name LIKE '%Imp%'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        rating,
        country_city,
        city_clean,
        rating_avg_6_prior,
        cumulative_rating,
        rating_bucket,
        cumulative_rating_2,
        CASE WHEN cumulative_rating >= 17 THEN 'High' WHEN cumulative_rating >= 9 THEN 'Medium' ELSE 'Low' END AS cumulative_rating_level
    FROM enriched
    WHERE rating_avg_6_prior > 3
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(rating_bucket) OVER (ORDER BY rating_bucket ASC, id ASC) AS prev_rating_bucket,
        LEAD(cumulative_rating) OVER (ORDER BY rating_bucket ASC, id ASC) AS next_cumulative_rating,
        LAG(cumulative_rating) OVER (ORDER BY rating_bucket ASC, id ASC) AS prev_cumulative_rating,
        ROW_NUMBER() OVER (ORDER BY rating_bucket ASC, id ASC) AS seq_no,
        LAST_VALUE(rating_bucket) IGNORE NULLS OVER (ORDER BY cumulative_rating_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_bucket
    FROM cleaned
),
cleaned_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country_city,
        city_clean,
        rating_avg_6_prior,
        cumulative_rating,
        rating_bucket,
        cumulative_rating_level,
        next_cumulative_rating,
        prev_cumulative_rating,
        last_known_rating_bucket,
        COALESCE(prev_cumulative_rating, 0) AS prev_cumulative_rating_filled,
        ROUND(prev_cumulative_rating, 2) AS prev_cumulative_rating_rounded
    FROM scored
    WHERE cumulative_rating_2 <= 11
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_cumulative_rating_rounded >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_cumulative_rating_rounded_tier,
        CASE WHEN last_known_rating_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS last_known_rating_bucket_tier,
        PERCENTILE_APPROX(prev_cumulative_rating, 0.9) AS prev_cumulative_rating_p90,
        APPROX_PERCENTILE(last_known_rating_bucket, 0.5) AS last_known_rating_bucket_p50,
        ROUND(AVG(last_known_rating_bucket), 2) AS avg_last_known_rating_bucket,
        ROUND(AVG(prev_cumulative_rating), 2) AS avg_prev_cumulative_rating
    FROM cleaned_suppliers
    GROUP BY GROUPING SETS ((CASE WHEN prev_cumulative_rating_rounded >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last_known_rating_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN prev_cumulative_rating_rounded >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END))
    HAVING ROUND(AVG(last_known_rating_bucket), 2) > 0
)
SELECT
    last_known_rating_bucket_tier,
    prev_cumulative_rating_p90,
    last_known_rating_bucket_p50,
    avg_last_known_rating_bucket,
    avg_prev_cumulative_rating
FROM summary
ORDER BY prev_cumulative_rating_p90 ASC;
