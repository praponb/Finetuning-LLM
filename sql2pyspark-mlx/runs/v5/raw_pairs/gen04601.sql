WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LEAST(quantity, unit_price) least_quantity_unit_price
    FROM order_items
    WHERE quantity BETWEEN 6 AND 8
),
enriched AS (
    -- Attach orders attributes
    SELECT a.*, d.status
    FROM filtered a
    JOIN orders d
        ON a.order_id = d.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        d.discount,
        d.channel
    FROM enriched a
    JOIN orders d
        ON a.order_id = d.id
    WHERE d.customer_id IS NULL
),
joined_order_items AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        p.shipping_fee,
        p.channel AS order_channel,
        p.status
    FROM joined s
    JOIN orders p
        ON s.order_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        shipping_fee,
        order_channel,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY order_channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status,
        ROW_NUMBER() OVER (PARTITION BY order_channel ORDER BY shipping_fee, id) event_seq
    FROM joined_order_items
)
SELECT
    s.id,
    s.order_id,
    s.shipping_fee,
    s.first_known_status,
    (SELECT SUM(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS sum_payment_amount
FROM with_windows s
ORDER BY shipping_fee DESC NULLS FIRST;
