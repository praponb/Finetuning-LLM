WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, customer_id ORDER BY amount DESC NULLS FIRST, id) AS rank_in_group
    FROM orders
    QUALIFY rank_in_group = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(order_date) order_date_month_end,
        CAST(discount AS INT) discount_int,
        COALESCE(channel, 'N/A') channel_filled
    FROM ranked
    WHERE customer_id IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'mobile'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, channel ORDER BY order_date_month_end, id) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.store_name,
        p.city
    FROM latest e
    INNER JOIN stores p
        ON e.store_id <=> p.id
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.payment_method
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.method AS payment_method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    COUNT_IF(city = 'London') AS london_events,
    CORR(amount, discount) AS corr_amount_discount,
    SUM(shipping_fee) AS total_shipping_fee,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
FROM with_last
GROUP BY year_start
ORDER BY london_events DESC NULLS FIRST
