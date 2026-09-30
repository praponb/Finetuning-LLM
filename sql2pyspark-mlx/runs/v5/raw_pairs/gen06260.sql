WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(city, 'NONE') AS resolved_city
    FROM suppliers
    WHERE supplier_name LIKE 'Lio%'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price >= 850
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        SUM(rating) OVER w_running AS running_rating_2,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM eligible
    WINDOW w_running AS (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            city,
            country,
            rating_share
        FROM with_windows
    ) src
    PIVOT (
        MAX(rating_share)
        FOR country IN ('JP', 'UK', 'TH')
    )
)
SELECT
    city,
    UK,
    CASE WHEN city RLIKE '^(Singapore|New York)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS city_zone
FROM by_type
ORDER BY city_zone DESC NULLS FIRST
