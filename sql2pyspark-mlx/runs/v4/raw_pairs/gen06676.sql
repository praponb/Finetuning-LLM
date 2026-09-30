WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN method = 'cash' THEN 1 ELSE 0 END AS is_cash
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-10-08 04:00:00')
      AND order_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.amount AS order_amount,
        d.shipping_fee,
        d.discount
    FROM base e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
base_payments AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(order_amount, 0) AS order_amount_filled
    FROM joined
    WHERE paid_at > TIMESTAMP '2023-09-20 13:00:00'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_cash DESC NULLS FIRST, id) AS row_num
    FROM base_payments
    QUALIFY row_num = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    order_id,
    method,
    paid_at,
    is_cash,
    shipping_fee,
    discount,
    order_amount_filled,
    row_num,
    CASE WHEN method RLIKE '^(promptpay|paypal)$' THEN 'PRIORITY' WHEN method RLIKE '^(card|bank_transfer)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS method_zone
FROM sessionized
ORDER BY method_zone DESC;
