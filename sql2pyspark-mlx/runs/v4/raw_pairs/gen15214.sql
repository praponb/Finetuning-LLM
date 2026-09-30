WITH kept_keys AS (
    SELECT country
    FROM suppliers
    EXCEPT
    SELECT country
    FROM suppliers
    WHERE rating >= 2.4
),
reconciled AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.country = k.country
),
scored AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        MIN(rating) OVER (PARTITION BY country) AS country_min_rating,
        MAX(rating) OVER (PARTITION BY city) AS city_max_rating,
        AVG(rating) OVER (PARTITION BY city) AS city_avg_rating
    FROM reconciled
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS city_avg_rating_dense_rank,
        PERCENT_RANK() OVER (ORDER BY city_max_rating DESC NULLS FIRST) AS city_max_rating_percent_rank,
        DENSE_RANK() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST) AS rating_dense_rank,
        MAX(city_max_rating) OVER w_recent AS city_max_city_max_rating,
        AVG(city_avg_rating) OVER w_recent AS city_avg_city_avg_rating
    FROM scored
    WINDOW w_hist AS (ORDER BY city_avg_rating NULLS FIRST), w_recent AS (PARTITION BY city)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost < 640
        )
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    country_min_rating,
    city_max_rating,
    city_avg_rating,
    city_avg_rating_dense_rank,
    city_max_rating_percent_rank,
    rating_dense_rank,
    city_max_city_max_rating,
    city_avg_city_avg_rating,
    RANK() OVER (PARTITION BY country ORDER BY city_avg_rating NULLS FIRST) AS city_avg_rating_rank,
    ROUND(city_avg_city_avg_rating / SUM(city_avg_city_avg_rating) OVER (PARTITION BY country), 4) AS city_avg_city_avg_rating_share,
    DENSE_RANK() OVER (ORDER BY rating DESC) AS rating_dense_rank_2
FROM eligible;
