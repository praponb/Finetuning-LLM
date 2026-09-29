WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating NULLS FIRST, id) AS rn
    FROM suppliers
    QUALIFY rn = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) AS latest_rank
        FROM ranked
    ) x
    WHERE latest_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NULL
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.category
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT country, id
    FROM latest_match
    INTERSECT ALL
    SELECT country, id
    FROM suppliers
    WHERE city = 'Chiang Mai'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN kept_keys k ON s.country = k.country AND s.id = k.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id = 39
        )
)
SELECT
    supplier_name,
    country,
    city,
    latest_rank
FROM eligible
ORDER BY city DESC;
