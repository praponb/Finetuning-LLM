WITH with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.amount
    FROM stores s
    JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft DESC, id) AS rn
    FROM kept
    QUALIFY rn = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (1, 4, 5, 6)
        )
)
-- Running and moving aggregates
SELECT
    id,
    store_name,
    opened_date,
    amount,
    SUM(sqft) OVER w_grp AS cumulative_sqft,
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft NULLS LAST) AS sqft_percent_rank,
    SUM(amount) OVER w_grp AS cumulative_amount
FROM not_flagged
WINDOW w_grp AS (PARTITION BY city ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
