WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(supplier_name, '/', country) AS supplier_name_country
    FROM suppliers
    WHERE city IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_cume_dist,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS seq_no
    FROM base
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(country, ', ', city) AS country_city,
        UPPER(supplier_name) AS supplier_name_upper
    FROM scored
    WHERE city IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_cume_dist DESC NULLS LAST, id) = 1
)
SELECT
    id,
    country,
    rating,
    supplier_name_country,
    rating_cume_dist,
    seq_no,
    country_city
FROM latest;
