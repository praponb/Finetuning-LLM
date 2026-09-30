WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS LAST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY city ORDER BY sqft, id) AS next_sqft,
        LAG(sqft) OVER (PARTITION BY city ORDER BY sqft, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS seq_no,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share
    FROM ranked
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.quantity
    FROM kept s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY sqft DESC NULLS LAST) AS sqft_percent_rank,
        LAST_VALUE(sqft_share, TRUE) OVER (ORDER BY next_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft_share,
        ROW_NUMBER() OVER w_hist AS seq_no_2,
        NTILE(4) OVER w_grp AS sqft_share_bucket
    FROM with_latest
    WHERE next_sqft < 13207
    WINDOW w_hist AS (PARTITION BY region ORDER BY quantity, id), w_grp AS (ORDER BY sqft_share, id)
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_amount,
        top1.order_date
    FROM scored s
    JOIN LATERAL (
            SELECT c.amount AS order_amount, c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM with_last
) src
PIVOT (
    MAX(sqft) AS value_sum,
    COUNT(1) AS cnt
    FOR region IN ('Central' AS central, 'West' AS west)
);
