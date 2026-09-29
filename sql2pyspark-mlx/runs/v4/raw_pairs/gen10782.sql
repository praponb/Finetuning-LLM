WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rating) OVER w_part AS cumulative_rating,
        AVG(rating) OVER (PARTITION BY country) AS country_avg_rating,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST) AS rating_dense_rank
    FROM suppliers
    WINDOW w_part AS (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY country, city ORDER BY rating_dense_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3,
        SUM(rating_dense_rank) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_dense_rank,
        AVG(rating_dense_rank) OVER w_hist AS city_avg_rating_dense_rank,
        ROUND(cumulative_rating / SUM(cumulative_rating) OVER (PARTITION BY country, city), 4) AS cumulative_rating_share
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY city, country)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating_dense_rank DESC) AS latest_rank
        FROM scored
    ) x
    WHERE latest_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM deduped
    EXCEPT ALL
    SELECT country, id
    FROM suppliers
    WHERE LENGTH(supplier_name) >= 13
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN matched_keys k ON s.country = k.country AND s.id = k.id
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT city, country
    FROM reconciled
    EXCEPT
    SELECT city, country
    FROM suppliers
    WHERE supplier_name IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN matched_keys_2 k ON s.city = k.city AND s.country = k.country
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    cumulative_rating,
    country_avg_rating,
    rating_dense_rank,
    rating_avg_3,
    city_avg_rating_dense_rank
FROM verified
ORDER BY city DESC, rating_avg_3 DESC
