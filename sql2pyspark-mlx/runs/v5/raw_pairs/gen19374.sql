WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', status, method) AS status_method_key,
        ROUND(amount * 1.15, 2) AS amount_adjusted
    FROM payments
    WHERE method = 'promptpay'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        method,
        amount,
        status
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY paid_at ASC NULLS LAST) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        MAX(amount) OVER w_part AS running_max_amount,
        AVG(amount) OVER w_part AS running_avg_amount,
        MIN(amount) OVER w_part AS running_min_amount,
        COUNT(amount) OVER w_part AS running_count_amount
    FROM latest
    WINDOW w_part AS (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.running_avg_amount,
        p.discount,
        p.shipping_fee
    FROM with_windows e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
report AS (
    SELECT
        id,
        order_id,
        running_avg_amount,
        discount,
        shipping_fee
    FROM enriched
)
SELECT
    id,
    running_avg_amount,
    discount,
    shipping_fee
FROM report
ORDER BY discount DESC NULLS FIRST
