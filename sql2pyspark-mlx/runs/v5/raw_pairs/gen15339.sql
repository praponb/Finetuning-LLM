WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-06-09 13:00:00'
),
cleaned_payments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount, 0) AS amount_rounded,
        COALESCE(method, 'OTHER') AS method_filled
    FROM cleaned
    WHERE paid_at >= DATE '2023-08-11'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.amount,
        e.status,
        e.status_title,
        e.method_filled,
        p.discount,
        p.channel,
        p.shipping_fee
    FROM cleaned_payments e
    JOIN orders p
        ON e.order_id = p.id
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.amount AS order_amount,
        o.order_date
    FROM with_dims a
    LEFT JOIN orders o
        ON a.order_id <=> o.id
    WHERE o.channel IN ('partner', 'store')
)
-- Join a dimension and aggregate
SELECT
    p.status AS order_status,
    e.channel AS payment_channel,
    COUNT(*) AS num_rows,
    MAX(e.shipping_fee) AS max_shipping_fee,
    MAX(e.discount) AS max_discount,
    GROUPING_ID() AS gid
FROM with_dims_payments e
LEFT JOIN orders p ON e.order_id = p.id
GROUP BY GROUPING SETS ((p.status, e.channel), (p.status), ())
ORDER BY num_rows NULLS LAST
