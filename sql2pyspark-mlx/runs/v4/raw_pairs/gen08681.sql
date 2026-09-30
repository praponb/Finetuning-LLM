WITH base AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT(city, '/', supplier_name) AS city_supplier_name
    FROM suppliers
    WHERE city IN ('Chiang Mai', 'New York')
),
with_windows AS (
    SELECT
        *,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior,
        RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_rank
    FROM base
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating_rank DESC NULLS LAST, id) = 1
),
with_windows_suppliers AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_supplier_name,
        rating_avg_2_prior,
        rating_rank,
        NTILE(3) OVER (ORDER BY rating_avg_2_prior DESC, id) AS rating_avg_2_prior_bucket,
        LAST_VALUE(rating_avg_2_prior) IGNORE NULLS OVER w_running AS last_known_rating_avg_2_prior,
        AVG(rating) OVER (PARTITION BY city, country) AS city_avg_rating,
        LAST_VALUE(rating) IGNORE NULLS OVER w_running AS last_known_rating,
        SUM(rating_rank) OVER (ORDER BY rating_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_rank
    FROM ranked
    WINDOW w_running AS (PARTITION BY city ORDER BY rating_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_suppliers_2 AS (
    SELECT
        *,
        ROUND(rating_rank / SUM(rating_rank) OVER (PARTITION BY country), 4) AS rating_rank_share,
        NTILE(4) OVER (PARTITION BY country ORDER BY rating_avg_2_prior DESC, id) AS rating_avg_2_prior_bucket_2,
        AVG(rating_rank) OVER (PARTITION BY country ORDER BY last_known_rating_avg_2_prior, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_rank_avg_6_prior,
        RANK() OVER (PARTITION BY country ORDER BY city_avg_rating NULLS LAST) AS city_avg_rating_rank
    FROM with_windows_suppliers
),
matched_keys AS (
    SELECT country
    FROM with_windows_suppliers_2
    INTERSECT ALL
    SELECT country
    FROM suppliers
    WHERE rating IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM with_windows_suppliers_2 s
    INNER JOIN matched_keys k ON s.country = k.country
),
with_windows_suppliers_3 AS (
    SELECT
        *,
        MAX(last_known_rating_avg_2_prior) OVER (PARTITION BY country) AS country_max_last_known_rating_avg_2_prior,
        STDDEV_SAMP(city_avg_rating_rank) OVER (PARTITION BY city) AS city_stddev_samp_city_avg_rating_rank
    FROM reconciled
)
SELECT
    country,
    city,
    rating,
    rating_rank,
    rating_avg_2_prior_bucket,
    city_avg_rating,
    running_rating_rank,
    rating_rank_share,
    city_avg_rating_rank
FROM with_windows_suppliers_3
