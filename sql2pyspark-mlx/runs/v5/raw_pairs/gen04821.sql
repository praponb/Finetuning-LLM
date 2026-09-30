WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(country) AS country_length
    FROM suppliers
    WHERE rating >= 3.6
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY country_length DESC NULLS FIRST, id ASC) = 1
),
ranked_suppliers AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC) AS latest_rank
        FROM ranked
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(rating) OVER w_ordered AS next_rating,
        LAG(country_length) OVER w_ordered AS prev_country_length,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM ranked_suppliers
    WINDOW w_ordered AS (PARTITION BY city ORDER BY country_length ASC, id ASC)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost < 470
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.country_length,
        s.latest_rank,
        top1.stock_qty
    FROM kept s
    JOIN LATERAL (
            SELECT c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    country_length,
    latest_rank
FROM latest_match
