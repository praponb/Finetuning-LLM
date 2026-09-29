WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) = 1
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.stock_qty
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY stock_qty DESC, id) AS latest_rank
        FROM latest_match
    ) x
    WHERE latest_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id = 4
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM kept
) src
PIVOT (
    MAX(rating) AS amount,
    COUNT(1) AS n
    FOR city IN ('Singapore' AS singapore, 'Berlin' AS berlin)
);
