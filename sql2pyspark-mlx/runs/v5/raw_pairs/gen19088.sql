WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        discount,
        CONCAT_WS(',', status, channel) AS status_channel_key,
        CONCAT_WS('/', status, channel) AS status_channel_key_2,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-03-03'
      AND order_date <= '2023-04-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS event_seq,
        SUM(discount) OVER (PARTITION BY store_id ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
        NTILE(3) OVER w_hist AS discount_bucket,
        MIN(discount) OVER (PARTITION BY status) AS status_min_discount
    FROM valid
    WINDOW w_grp AS (ORDER BY discount DESC, id ASC), w_hist AS (PARTITION BY store_id, status ORDER BY discount DESC, id ASC)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(discount_bucket, TRUE) OVER (PARTITION BY store_id ORDER BY discount_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_bucket,
        AVG(status_min_discount) OVER (PARTITION BY customer_id, status ORDER BY status_min_discount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_min_discount_avg_3
    FROM with_windows
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
kept_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IS NULL
        )
),
with_windows_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(discount) OVER (PARTITION BY store_id ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount,
        SUM(discount) OVER (PARTITION BY store_id ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount,
        AVG(discount) OVER (PARTITION BY store_id ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount,
        AVG(last_known_discount_bucket) OVER (PARTITION BY store_id ORDER BY discount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last_known_discount_bucket_avg_3_prior
    FROM kept_orders
    WHERE discount_bucket < 5
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows_orders s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
        )
)
-- Window metrics over several different partitions
SELECT
    *,
    LEAD(running_discount) OVER (PARTITION BY customer_id ORDER BY order_date ASC, id ASC) AS next_running_discount,
    LEAD(status_min_discount) OVER (PARTITION BY customer_id ORDER BY order_date ASC, id ASC) AS next_status_min_discount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC, id ASC) AS row_seq
FROM not_flagged
