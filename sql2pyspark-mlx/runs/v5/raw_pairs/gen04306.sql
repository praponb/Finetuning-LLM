WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        shipping_fee,
        CASE WHEN shipping_fee >= 41 THEN 'High' WHEN shipping_fee >= 11 THEN 'Medium' ELSE 'Low' END shipping_fee_level,
        UPPER(channel) channel_upper
    FROM orders
    WHERE store_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount) OVER w_recent next_amount,
        LAG(amount) OVER w_recent prev_amount,
        LAG(shipping_fee) OVER w_recent prev_shipping_fee,
        ROW_NUMBER() OVER w_recent row_seq
    FROM valid
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY amount ASC, id ASC)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.status IN ('SUCCESS', 'FAILED', 'REFUNDED')
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at < s.order_date
        )
)
SELECT
    id,
    customer_id,
    channel,
    amount,
    shipping_fee,
    shipping_fee_level,
    next_amount,
    prev_shipping_fee,
    row_seq,
    CASE WHEN channel RLIKE '^(web|mobile)$' THEN 'EXTENDED' WHEN channel RLIKE '^(store|partner)$' THEN 'CORE' ELSE 'PRIORITY' END AS channel_zone
FROM not_flagged
ORDER BY customer_id DESC NULLS LAST;
