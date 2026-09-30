WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(country) AS country_length
    FROM suppliers
    WHERE city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IN (29, 40)
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY country_length DESC, id) AS row_seq,
        RANK() OVER (ORDER BY country_length DESC NULLS FIRST) AS country_length_rank,
        MAX(rating) OVER (PARTITION BY city) AS city_max_rating
    FROM kept
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, rating - city_max_rating AS rating_minus_city_max_rating
    FROM metrics
    WHERE country IN ('SG', 'JP', 'DE')
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    COUNT_IF(country = 'JP') AS jp_events,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SUM(rating_minus_city_max_rating) AS total_rating_minus_city_max_rating,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM valid
GROUP BY CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING jp_events < 1
ORDER BY row_seq_tier DESC NULLS FIRST
