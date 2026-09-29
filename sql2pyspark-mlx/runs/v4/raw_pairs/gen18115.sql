WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(city, ' - ', supplier_name) AS city_supplier_name,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE rating BETWEEN 2.6 AND 3.5
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_supplier_name,
        city_clean,
        LAG(rating, 2) OVER (PARTITION BY country, city ORDER BY rating, id) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) AS row_seq
    FROM base
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(country)) AS country_title
    FROM with_windows
    WHERE rating BETWEEN 2.2 AND 2.3
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(row_seq) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS row_seq_avg_6,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        ROUND(prev_rating / SUM(prev_rating) OVER (PARTITION BY country), 4) AS prev_rating_share,
        LAST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating,
        LEAD(row_seq, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_row_seq
    FROM cleaned
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY prev_rating_share NULLS LAST) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM deduped
    EXCEPT
    SELECT city, id
    FROM suppliers
    WHERE rating < 2.9
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    JOIN kept_keys k ON s.city = k.city AND s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_supplier_name,
        city_clean,
        prev_rating,
        row_seq,
        country_title,
        row_seq_avg_6,
        rating_share,
        prev_rating_share,
        last_known_rating,
        next_row_seq,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city, country), 4) AS rating_share_2,
        ROUND(prev_rating / SUM(prev_rating) OVER (PARTITION BY country, city), 4) AS prev_rating_share_2
    FROM kept_rows
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        city,
        prev_rating_share_2,
        prev_rating
    FROM enriched
) src
UNPIVOT (
    value FOR measure IN (prev_rating_share_2 AS PREV_RATING_SHARE_2, prev_rating)
)
