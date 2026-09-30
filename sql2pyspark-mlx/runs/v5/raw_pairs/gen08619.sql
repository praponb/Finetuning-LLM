WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        order_date,
        status,
        discount,
        TRIM(channel) AS clean_channel
    FROM orders
    WHERE order_date > DATE '2023-04-04'
      AND store_id IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        order_date,
        status,
        discount,
        clean_channel,
        CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END AS is_pending
    FROM filtered
    WHERE discount >= 0.38
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(discount) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS prev_discount,
        LEAD(is_pending) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS next_is_pending,
        LAG(is_pending) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS prev_is_pending,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS event_seq
    FROM base
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY next_is_pending) = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.email,
        p.city,
        p.country
    FROM ranked src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.clean_channel,
    s.is_pending,
    s.prev_is_pending,
    s.event_seq,
    s.city,
    (SELECT MIN(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM enriched s
