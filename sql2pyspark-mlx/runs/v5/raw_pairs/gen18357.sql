WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft
    FROM stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date ASC NULLS FIRST, id ASC) = 1
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_channel,
        top1.status
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.channel AS order_channel, c.status
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(sqft) OVER (PARTITION BY city, status ORDER BY opened_date ASC, id ASC) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY city, status ORDER BY opened_date ASC, id ASC) AS row_seq,
        NTILE(4) OVER (ORDER BY sqft DESC, id ASC) AS sqft_bucket
    FROM latest_match
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id IN (1, 2, 7, 8)
        )
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        row_seq + prev_sqft AS row_seq_plus_prev_sqft,
        LOWER(order_channel) AS order_channel_lower
    FROM eligible
    WHERE opened_date <= '2023-10-06'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        opened_date,
        prev_sqft,
        row_seq,
        sqft_bucket,
        row_seq_plus_prev_sqft,
        order_channel_lower
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, region ORDER BY row_seq_plus_prev_sqft DESC NULLS LAST, id ASC) = 1
)
-- Window metrics over several different partitions
SELECT
    id,
    order_channel_lower,
    MAX(row_seq_plus_prev_sqft) OVER (ORDER BY prev_sqft ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_row_seq_plus_prev_sqft,
    AVG(row_seq_plus_prev_sqft) OVER (ORDER BY prev_sqft ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_row_seq_plus_prev_sqft,
    MIN(row_seq_plus_prev_sqft) OVER (ORDER BY prev_sqft ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_row_seq_plus_prev_sqft,
    LEAD(prev_sqft, 1) OVER (ORDER BY row_seq ASC, id ASC) AS next_prev_sqft
FROM latest
WHERE row_seq = 7;
