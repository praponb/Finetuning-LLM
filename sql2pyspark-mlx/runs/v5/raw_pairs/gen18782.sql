WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'web'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY days_since_order_date, id) AS days_since_order_date_bucket,
        SUM(shipping_fee) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_shipping_fee
    FROM filtered
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.amount,
        latest.method
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        days_since_order_date_bucket - amount AS days_since_order_date_bucket_minus_amount,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM with_latest
    WHERE order_date >= DATE '2023-09-04'
      AND customer_id IN (27, 38)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY days_since_order_date_bucket_minus_amount, id) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
scored_orders AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY channel ORDER BY cumulative_shipping_fee, id) AS cumulative_shipping_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cumulative_shipping_fee, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY status, store_id ORDER BY days_since_order_date_bucket_minus_amount DESC NULLS FIRST) AS days_since_order_date_bucket_minus_amount_cume_dist,
        LAST_VALUE(days_since_order_date_bucket_minus_amount) IGNORE NULLS OVER (PARTITION BY store_id, status ORDER BY days_since_order_date_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_order_date_bucket_minus_amount
    FROM deduped
    WHERE order_date > '2023-08-10'
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.shipped_date
    FROM scored_orders s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Running and moving aggregates
SELECT
    id,
    store_id,
    channel,
    discount,
    days_since_order_date_bucket,
    cumulative_shipping_fee,
    amount,
    latest_rank,
    last_known_days_since_order_date_bucket_minus_amount,
    AVG(days_since_order_date_bucket_minus_amount) OVER w_running AS running_avg_days_since_order_date_bucket_minus_amount,
    MAX(days_since_order_date_bucket_minus_amount) OVER w_running AS running_max_days_since_order_date_bucket_minus_amount,
    COUNT(days_since_order_date_bucket_minus_amount) OVER w_running AS running_count_days_since_order_date_bucket_minus_amount
FROM with_last
WINDOW w_running AS (PARTITION BY channel ORDER BY days_since_order_date_bucket_minus_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
