WITH kept_keys AS (
    -- Set difference on keys
    SELECT city
    FROM suppliers
    EXCEPT DISTINCT
    SELECT city
    FROM suppliers
    WHERE rating > 2.0
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    INNER JOIN kept_keys k ON s.city = k.city
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        rating,
        COALESCE(rating, 0) AS rating_filled
    FROM reconciled
    WHERE city IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY country) AS country_sum_rating,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST) AS rating_percent_rank
    FROM cleaned
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ARRAY_CONTAINS(ch.tags, 'bestseller')
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty >= 147
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        country,
        rating_filled,
        rating
    FROM not_flagged
) src
UNPIVOT (
    value FOR kpi IN (rating_filled, rating)
);
