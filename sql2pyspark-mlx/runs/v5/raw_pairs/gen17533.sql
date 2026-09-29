WITH cleaned AS (
    SELECT
        *,
        CASE WHEN rating >= 4.1 THEN 'High' WHEN rating >= 2.4 THEN 'Medium' ELSE 'Low' END AS rating_level,
        LENGTH(country) AS country_length
    FROM suppliers
    WHERE LOWER(TRIM(supplier_name)) = 'global supply'
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date < '2024-08-22'
        )
),
metrics AS (
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        ROW_NUMBER() OVER w_recent AS event_seq,
        LAST_VALUE(rating, TRUE) OVER w_ordered AS last_known_rating,
        AVG(country_length) OVER (PARTITION BY city ORDER BY country_length, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS country_length_avg_5_prior,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share_2
    FROM kept
    WINDOW w_recent AS (ORDER BY country_length DESC, id), w_ordered AS (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    SELECT city, id
    FROM metrics
    EXCEPT ALL
    SELECT city, id
    FROM suppliers
    WHERE rating >= 2.9
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
with_windows AS (
    SELECT
        *,
        STDDEV_SAMP(rating_share_2) OVER w_recent AS country_stddev_samp_rating_share_2,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY country_length_avg_5_prior NULLS FIRST) AS country_length_avg_5_prior_percent_rank,
        DENSE_RANK() OVER (ORDER BY rating NULLS FIRST) AS rating_dense_rank,
        RANK() OVER (ORDER BY country_length_avg_5_prior DESC NULLS FIRST) AS country_length_avg_5_prior_rank,
        MAX(rating_share) OVER w_recent AS country_max_rating_share
    FROM verified
    WHERE country_length_avg_5_prior > 70
    WINDOW w_recent AS (PARTITION BY country)
),
kept_suppliers AS (
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.product_name LIKE 'Cof%'
        )
),
kept_suppliers_2 AS (
    SELECT s.*
    FROM kept_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND UPPER(TRIM(x.product_name)) = 'DESK LAMP'
        )
)
SELECT *
FROM (
    SELECT
        country,
        city,
        country_max_rating_share
    FROM kept_suppliers_2
) src
PIVOT (
    MAX(country_max_rating_share)
    FOR city IN ('New York' AS new_york, 'London' AS london, 'Chiang Mai' AS chiang_mai)
);
