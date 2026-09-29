WITH base AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        SUBSTRING(city, 1, 2) AS city_prefix,
        INITCAP(TRIM(supplier_name)) AS supplier_name_title
    FROM suppliers
    WHERE city NOT IN ('London', 'Singapore', 'Chiang Mai')
),
scored AS (
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        AVG(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior,
        NTILE(10) OVER (PARTITION BY country ORDER BY rating DESC, id) AS rating_bucket,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_2,
        MIN(rating) OVER (PARTITION BY country, city) AS country_min_rating
    FROM base
),
enriched AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY country ORDER BY rating_bucket DESC, id) AS rating_bucket_bucket,
        ROW_NUMBER() OVER w_recent AS row_seq,
        LEAD(country_min_rating, 2) OVER (ORDER BY cumulative_rating_2, id) AS next_country_min_rating
    FROM scored
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating_bucket, id)
),
scored_suppliers AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_prefix,
        supplier_name_title,
        cumulative_rating,
        rating_avg_5_prior,
        rating_bucket,
        cumulative_rating_2,
        country_min_rating,
        rating_bucket_bucket,
        row_seq,
        next_country_min_rating,
        LAST_VALUE(next_country_min_rating, TRUE) OVER (PARTITION BY city ORDER BY next_country_min_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_country_min_rating,
        ROUND(country_min_rating / SUM(country_min_rating) OVER (PARTITION BY city, country), 4) AS country_min_rating_share,
        STDDEV_SAMP(rating) OVER (PARTITION BY city) AS city_stddev_samp_rating
    FROM enriched
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    city_prefix,
    supplier_name_title,
    cumulative_rating,
    rating_bucket,
    rating_bucket_bucket
FROM scored_suppliers;
