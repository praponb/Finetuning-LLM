WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(supplier_name, 'unassigned') AS supplier_name_filled,
        FLOOR(rating) AS rating_whole
    FROM suppliers
    WHERE rating < 2.4
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        country,
        rating,
        rating_whole
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating_whole DESC, id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date <= '2025-05-01'
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) AS rn
        FROM kept
    ) x
    WHERE rn = 1
)
SELECT
    id,
    country,
    rating,
    rating_whole,
    rn,
    CASE WHEN country RLIKE '^(UK|US)$' THEN 'CORE' WHEN country RLIKE '^(TH|SG)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS country_zone
FROM ranked
ORDER BY country DESC;
