WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        ROUND(amount, 2) AS amount_rounded,
        COALESCE(method, 'N/A') AS method_clean
    FROM payments
    WHERE paid_at > DATE '2023-08-12'
      AND order_id = 16
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(amount) IGNORE NULLS OVER w_running AS last_known_amount,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY amount_rounded DESC NULLS LAST) AS amount_rounded_dense_rank,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_2
    FROM filtered
    WHERE TO_DATE(paid_at) >= '2024-12-28'
    WINDOW w_running AS (PARTITION BY order_id ORDER BY amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.shipping_fee,
        p.status AS order_status
    FROM enriched src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
),
enriched_payments_2 AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.amount,
        s.paid_at,
        s.status,
        s.amount_rounded,
        s.last_known_amount,
        s.last_known_amount_2,
        s.shipping_fee,
        o.channel,
        o.shipping_fee AS order_shipping_fee
    FROM enriched_payments s
    INNER JOIN orders o
        ON s.order_id = o.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched_payments_2
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        d.discount,
        d.shipping_fee AS order_shipping_fee_2,
        d.status AS order_status
    FROM sessions_payments src
    INNER JOIN orders d
        ON src.order_id <=> d.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.paid_at,
        src.status,
        src.shipping_fee,
        src.order_shipping_fee,
        src.discount,
        src.order_shipping_fee_2,
        b.shipping_fee AS order_shipping_fee_3
    FROM with_dims src
    LEFT JOIN orders b
        ON src.order_id = b.id
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    s.status,
    s.discount,
    s.order_shipping_fee_2,
    (SELECT SUM(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS sum_payment_amount
FROM with_dims_payments s;
