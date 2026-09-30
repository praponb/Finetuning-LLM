WITH matched_keys AS (
    -- Set intersection on keys
    SELECT id, country
    FROM suppliers
    INTERSECT
    SELECT id, country
    FROM suppliers
    WHERE country IN ('TH', 'JP', 'US')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_percent_rank,
        CUME_DIST() OVER (ORDER BY rating ASC) AS rating_cume_dist,
        RANK() OVER (PARTITION BY city, country ORDER BY rating ASC NULLS FIRST) AS rating_rank
    FROM reconciled
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.product_name LIKE 'W%'
        )
),
metrics_suppliers AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY rating DESC NULLS FIRST) AS rating_rank_2,
        COUNT(rating_rank) OVER (PARTITION BY country) AS country_count_rating_rank,
        DENSE_RANK() OVER w_running AS rating_cume_dist_dense_rank,
        ROUND(rating_rank / SUM(rating_rank) OVER (PARTITION BY country, city), 4) AS rating_rank_share
    FROM kept
    WINDOW w_running AS (PARTITION BY country, city ORDER BY rating_cume_dist DESC NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        supplier_name,
        country,
        rating,
        rating_percent_rank,
        rating_rank_2,
        rating_cume_dist_dense_rank,
        rating_rank_share,
        rating_cume_dist - rating_cume_dist_dense_rank AS rating_cume_dist_minus_rating_cume_dist_dense_rank
    FROM metrics_suppliers
    WHERE rating_rank_share IS NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(rating_rank_share, rating_cume_dist_dense_rank) AS greatest_rating_rank_share_rating_cume_dist_dense_rank
    FROM valid
    WHERE rating_rank_2 > 7
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        supplier_name,
        country,
        rating,
        rating_percent_rank,
        rating_rank_2,
        rating_cume_dist_dense_rank,
        rating_rank_share,
        rating_cume_dist_minus_rating_cume_dist_dense_rank,
        greatest_rating_rank_share_rating_cume_dist_dense_rank,
        STDDEV_SAMP(greatest_rating_rank_share_rating_cume_dist_dense_rank) OVER w_hist AS country_stddev_samp_greatest_rating_rank_share_rating_cume_dist_dense_rank,
        COUNT(rating_cume_dist_minus_rating_cume_dist_dense_rank) OVER w_hist AS country_count_rating_cume_dist_minus_rating_cume_dist_dense_rank,
        AVG(rating_rank_2) OVER w_hist AS country_avg_rating_rank_2
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY country)
)
-- Grouped metrics
SELECT
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT_IF(country = 'JP') AS jp_events
FROM enriched
GROUP BY 1
ORDER BY country DESC
