WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(country) clean_country,
        CONCAT_WS('-', supplier_name, country) supplier_name_country_key,
        COALESCE(city, 'UNKNOWN') city_clean
    FROM suppliers
    WHERE supplier_name LIKE '%Globa%'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_rating,
        COUNT(rating) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_rating,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_rating,
        MAX(rating) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_rating
    FROM cleaned
    WHERE supplier_name LIKE '%Gmb%'
),
cleaned_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(last3_sum_rating) last3_sum_rating_whole
    FROM scored
    WHERE rating < 2.0
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        rating,
        city_clean,
        last3_max_rating,
        FIRST_VALUE(rating) IGNORE NULLS OVER (ORDER BY last3_sum_rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_rating,
        LEAD(last3_sum_rating_whole, 2) OVER (ORDER BY last3_max_rating ASC, id ASC) next_last3_sum_rating_whole,
        CUME_DIST() OVER (PARTITION BY country, city ORDER BY last3_avg_rating DESC NULLS LAST) last3_avg_rating_cume_dist,
        ROW_NUMBER() OVER w_recent seq_no,
        NTILE(4) OVER (PARTITION BY city ORDER BY last3_max_rating ASC, id ASC) last3_max_rating_bucket
    FROM cleaned_suppliers
    WINDOW w_recent AS (PARTITION BY country ORDER BY last3_max_rating ASC, id ASC)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last3_max_rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS last3_max_rating_tier,
    CASE WHEN rating >= 4.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    SUM(CASE WHEN country = 'TH' THEN seq_no ELSE 0 END) AS th_seq_no,
    APPROX_PERCENTILE(last3_avg_rating_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS last3_avg_rating_cume_dist_quartiles
FROM enriched
GROUP BY last3_max_rating_tier, rating_tier
ORDER BY sg_events ASC;
