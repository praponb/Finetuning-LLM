WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        rating,
        COALESCE(city, 'Unknown') AS city_filled,
        CAST(rating AS INT) AS rating_int
    FROM suppliers
    WHERE city IN ('Berlin', 'Bangkok', 'Singapore')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost > 670
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating_int) OVER (ORDER BY rating_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_int,
        ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS event_seq,
        PERCENT_RANK() OVER w_running AS rating_int_percent_rank
    FROM eligible
    WINDOW w_running AS (ORDER BY rating_int NULLS LAST)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT ALL
    SELECT id
    FROM suppliers
    WHERE rating >= 2.0
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty BETWEEN 250 AND 272
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        rating,
        city_filled,
        rating_int,
        running_rating_int,
        event_seq,
        rating_int_percent_rank,
        CUME_DIST() OVER (ORDER BY rating NULLS LAST) AS rating_cume_dist,
        PERCENT_RANK() OVER (ORDER BY rating_int NULLS FIRST) AS rating_int_percent_rank_2
    FROM kept
),
base_suppliers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        rating,
        rating_int,
        running_rating_int,
        event_seq,
        rating_cume_dist,
        rating_int_percent_rank_2,
        running_rating_int + rating_int AS running_rating_int_plus_rating_int
    FROM with_windows
    WHERE rating_int_percent_rank > 0.63
)
SELECT
    id,
    rating,
    rating_int,
    running_rating_int,
    rating_int_percent_rank_2,
    running_rating_int_plus_rating_int
FROM base_suppliers
ORDER BY running_rating_int NULLS FIRST
