WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(shipping_fee) OVER w_recent AS prev_shipping_fee,
        LEAD(amount) OVER w_recent AS next_amount,
        LAG(amount) OVER w_recent AS prev_amount,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM orders
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY shipping_fee ASC, id ASC)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity < 5
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        status,
        amount,
        discount,
        next_amount,
        prev_amount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount ASC, id ASC) AS rank_in_group
        FROM not_flagged
    ) x
    WHERE rank_in_group = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        status,
        discount,
        next_amount,
        prev_amount,
        ROUND(discount / next_amount, 2) AS discount_per_next_amount
    FROM deduped
    WHERE amount > 530
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        discount,
        discount_per_next_amount,
        NTILE(5) OVER (PARTITION BY status ORDER BY discount ASC, id ASC) AS discount_bucket,
        SUM(discount_per_next_amount) OVER (PARTITION BY status ORDER BY prev_amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_per_next_amount,
        LEAD(discount_per_next_amount, 1) OVER w_running AS next_discount_per_next_amount
    FROM base
    WHERE prev_amount <= 530
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY prev_amount ASC, id ASC)
)
-- Multi-level aggregation
SELECT
    customer_id,
    APPROX_PERCENTILE(discount_per_next_amount, 0.5) AS discount_per_next_amount_p50,
    PERCENTILE_APPROX(discount_bucket, ARRAY(0.25, 0.5, 0.75)) AS discount_bucket_quartiles,
    COUNT(*) AS cnt,
    ROUND(AVG(discount), 2) AS avg_discount
FROM enriched
GROUP BY 1
ORDER BY discount_per_next_amount_p50 ASC
