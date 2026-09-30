WITH latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.launch_date
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        launch_date,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        LAG(rating, 1) OVER w_hist AS prev_rating,
        AVG(rating) OVER w_part AS rating_avg_6_prior,
        PERCENT_RANK() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST) AS rating_percent_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share
    FROM latest_match
    WINDOW w_hist AS (ORDER BY launch_date, id), w_part AS (ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= s.launch_date
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price BETWEEN 340 AND 920
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date <= s.launch_date
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        launch_date,
        first_known_city,
        prev_rating,
        rating_avg_6_prior,
        rating_percent_rank,
        rating_share,
        ROUND(prev_rating / SUM(prev_rating) OVER (PARTITION BY country), 4) AS prev_rating_share,
        LAG(rating, 1) OVER (ORDER BY rating_percent_rank, id) AS prev_rating_2
    FROM not_flagged
),
remaining_keys AS (
    -- Set difference on keys
    SELECT city, country
    FROM metrics
    EXCEPT
    SELECT city, country
    FROM suppliers
    WHERE rating > 3.5
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.country = k.country
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    launch_date,
    prev_rating,
    rating_avg_6_prior,
    rating_percent_rank,
    prev_rating_share
FROM verified
ORDER BY prev_rating_share DESC NULLS LAST
