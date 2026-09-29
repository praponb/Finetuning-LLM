WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(country, 1, 2) AS country_prefix,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE rating BETWEEN 2.5 AND 3.4
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.cost
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rank_in_group
        FROM with_latest
    ) x
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date BETWEEN '2024-01-04' AND '2025-08-01'
        )
)
SELECT
    id,
    country,
    city,
    rating,
    country_prefix,
    city_filled,
    cost,
    rank_in_group
FROM not_flagged
ORDER BY rating, cost
