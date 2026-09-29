WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, channel ORDER BY order_date DESC, id) AS latest_rank
        FROM orders
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY channel, store_id ORDER BY latest_rank, id) AS row_seq,
        ROW_NUMBER() OVER w_recent AS event_seq,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS amount_percent_rank
    FROM latest
    WINDOW w_recent AS (ORDER BY latest_rank DESC, id)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.credit_limit,
        p.segment
    FROM metrics a
    JOIN customers p
        ON a.customer_id = p.id
),
latest_orders AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date, id) = 1
),
latest_orders_2 AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY event_seq DESC, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        latest_rank,
        row_seq,
        event_seq,
        amount_percent_rank,
        credit_limit,
        segment,
        NTILE(3) OVER (PARTITION BY segment ORDER BY discount, id) AS discount_bucket,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_percent_rank,
        FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY channel, status ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee
    FROM latest_orders_2
)
-- Aggregate with conditional metrics
SELECT
    segment,
    COUNT(DISTINCT channel) AS distinct_channels,
    SIZE(COLLECT_SET(segment)) AS num_segments
FROM with_windows
GROUP BY segment
ORDER BY distinct_channels DESC NULLS LAST
