WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(TRIM(supplier_name)) AS supplier_name_normalized,
        COALESCE(city, 'UNKNOWN') AS resolved_city
    FROM suppliers
    WHERE city IN ('Singapore', 'Berlin', 'Bangkok')
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.brand
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost > 670
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty > 233
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(rating) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS seq_no
    FROM not_flagged
)
-- Window metrics over several different partitions
SELECT
    *,
    LEAD(rating) OVER (PARTITION BY brand ORDER BY rating, id) AS next_rating,
    LAG(rating) OVER (PARTITION BY brand ORDER BY rating, id) AS prev_rating_2,
    LAG(prev_rating) OVER (PARTITION BY brand ORDER BY rating, id) AS prev_prev_rating,
    LEAD(prev_rating) OVER (PARTITION BY brand ORDER BY rating, id) AS next_prev_rating,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY rating, id) AS row_seq
FROM metrics;
