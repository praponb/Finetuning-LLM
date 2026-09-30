WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        amount * discount AS amount_times_discount,
        ADD_MONTHS(order_date, 1) AS order_date_plus_months
    FROM orders
    WHERE channel IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date >= s.order_date
        )
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.order_date_plus_months,
        x.carrier
    FROM eligible s
    JOIN LATERAL (
            SELECT c.carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_plus_months,
        carrier,
        discount - shipping_fee AS discount_minus_shipping_fee
    FROM with_last
    WHERE carrier = 'FedEx'
),
eligible_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at <= s.order_date_plus_months
        )
)
SELECT
    id,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    order_date_plus_months,
    carrier,
    discount_minus_shipping_fee
FROM eligible_orders
ORDER BY status
