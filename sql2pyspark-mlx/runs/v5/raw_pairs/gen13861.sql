WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM suppliers
    EXCEPT
    SELECT id, city
    FROM suppliers
    WHERE city IN ('Singapore', 'Bangkok', 'Berlin')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Acme', 'Globex')
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY rating DESC NULLS FIRST) AS rating_cume_dist,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        MIN(rating) OVER (PARTITION BY country) AS country_min_rating
    FROM eligible
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price < 410
        )
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    rating_cume_dist,
    rating_share,
    country_min_rating,
    CASE WHEN country RLIKE '^(SG|US)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|JP)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS country_zone
FROM kept
ORDER BY country DESC NULLS LAST;
