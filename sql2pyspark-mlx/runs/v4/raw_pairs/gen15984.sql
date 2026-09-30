WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(country, '/', supplier_name) AS country_supplier_name,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NULL
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_supplier_name,
        resolved_city,
        FIRST_VALUE(rating) IGNORE NULLS OVER w_hist AS first_known_rating,
        FIRST_VALUE(city, TRUE) OVER w_hist AS first_known_city
    FROM eligible
    WINDOW w_hist AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND LENGTH(x.product_name) > 14
        )
)
-- Ranking and gap features
SELECT
    *,
    LAG(first_known_rating, 1, 0) OVER w_part AS prev_first_known_rating,
    LAG(first_known_rating, 1) OVER (ORDER BY first_known_rating, id) AS prev_first_known_rating_2
FROM not_flagged
WINDOW w_part AS (PARTITION BY country ORDER BY rating, id)
