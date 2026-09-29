WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock >= s.opened_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock <= s.opened_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share,
        LAG(sqft, 1) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_sqft,
        ROW_NUMBER() OVER w_part AS seq_no,
        NTILE(10) OVER (ORDER BY sqft DESC, id) AS sqft_bucket
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY city ORDER BY opened_date, id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY seq_no DESC NULLS FIRST, id) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY prev_sqft DESC, id) = 1
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.sqft_share,
        s.seq_no,
        s.sqft_bucket,
        x.amount,
        x.order_order_date
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.order_date AS order_order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    city,
    seq_no,
    sqft_bucket,
    amount
FROM latest_match
ORDER BY amount DESC
