WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        ROUND(discount, 0) AS discount_rounded,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM orders
    WHERE status NOT IN ('COMPLETED', 'CANCELLED', 'PENDING')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, c.credit_limit
    FROM latest a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        store_id,
        CASE WHEN discount_rounded >= 61 THEN 'HIGH' ELSE 'LOW' END AS discount_rounded_tier,
        COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
        COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
        ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS shipping_fee_per_store,
        SUM(discount) AS total_discount
    FROM joined
    GROUP BY 1, 2
)
SELECT
    store_id,
    discount_rounded_tier,
    store_count
FROM stats
ORDER BY store_count DESC;
