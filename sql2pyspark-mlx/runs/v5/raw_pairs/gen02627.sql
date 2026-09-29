WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.method = 'promptpay'
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        AVG(shipping_fee) OVER w_part shipping_fee_avg_2,
        PERCENT_RANK() OVER w_part_2 shipping_fee_percent_rank,
        LAG(amount, 1) OVER (PARTITION BY store_id ORDER BY order_date, id) prev_amount
    FROM eligible
    WINDOW w_part AS (ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY customer_id ORDER BY shipping_fee NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', status, channel) status_channel_key
    FROM scored
    WHERE order_date > DATE '2023-07-07'
      AND customer_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM valid
    EXCEPT ALL
    SELECT store_id
    FROM orders
    WHERE amount < 1100
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.store_id = k.store_id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        store_id,
        status,
        prev_amount
    FROM kept_rows
) src
PIVOT (
    AVG(prev_amount)
    FOR status IN ('RETURNED', 'PENDING', 'SHIPPED')
);
