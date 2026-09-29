WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END AS is_bank_transfer
    FROM payments
    WHERE amount BETWEEN 700 AND 720
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.shipping_fee,
        d.discount
    FROM filtered s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY order_id, status ORDER BY is_bank_transfer, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        SUM(shipping_fee) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
    FROM with_dims
),
with_dims_payments AS (
    -- Attach orders attributes
    SELECT src.*, o.amount AS order_amount
    FROM metrics src
    JOIN orders o
        ON src.order_id = o.id
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.status,
    s.is_bank_transfer,
    s.shipping_fee,
    s.discount,
    s.running_shipping_fee,
    CASE WHEN method RLIKE '^(cash|paypal)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS method_zone,
    (SELECT MAX(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS max_order_item_unit_price
FROM with_dims_payments s
