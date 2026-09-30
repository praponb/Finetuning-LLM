WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(channel, '/', status) AS channel_status
    FROM orders
    WHERE customer_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
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
        channel_status,
        latest_rank,
        LEAD(discount, 2) OVER (PARTITION BY customer_id ORDER BY latest_rank, id) AS next_discount,
        AVG(shipping_fee) OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_5
    FROM ranked
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.signup_date,
        p.name
    FROM with_windows src
    JOIN customers p
        ON src.customer_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    SUM(amount) FILTER (WHERE status = 'CANCELLED') AS cancelled_total_amount,
    CORR(shipping_fee, amount) AS corr_shipping_fee_amount,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT_IF(status = 'SHIPPED') AS shipped_events,
    SUM(CASE WHEN status = 'PENDING' THEN discount ELSE 0 END) AS pending_discount
FROM with_dims
GROUP BY customer_id
ORDER BY shipped_events DESC NULLS FIRST
