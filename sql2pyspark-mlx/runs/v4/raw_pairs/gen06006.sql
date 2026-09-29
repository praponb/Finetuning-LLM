WITH base AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        shipping_fee,
        FLOOR(amount) AS amount_whole,
        shipping_fee - discount AS shipping_fee_minus_discount,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE order_date > DATE '2024-02-09'
      AND amount BETWEEN 870 AND 1500
),
base_orders AS (
    SELECT
        id,
        status,
        channel,
        amount_whole,
        shipping_fee_minus_discount,
        channel_clean,
        FLOOR(shipping_fee_minus_discount) AS shipping_fee_minus_discount_whole,
        shipping_fee_minus_discount - shipping_fee AS shipping_fee_minus_discount_minus_shipping_fee
    FROM base
    WHERE customer_id IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        status,
        channel,
        amount_whole,
        shipping_fee_minus_discount,
        channel_clean,
        shipping_fee_minus_discount_whole,
        shipping_fee_minus_discount_whole / NULLIF(shipping_fee_minus_discount_minus_shipping_fee, 0) AS shipping_fee_minus_discount_whole_per_shipping_fee_minus_discount_minus_shipping_fee,
        COALESCE(shipping_fee_minus_discount_whole, 0) AS shipping_fee_minus_discount_whole_filled
    FROM base_orders
    WHERE channel IS NOT NULL
)
SELECT
    id,
    status,
    amount_whole,
    shipping_fee_minus_discount,
    shipping_fee_minus_discount_whole,
    shipping_fee_minus_discount_whole_per_shipping_fee_minus_discount_minus_shipping_fee,
    shipping_fee_minus_discount_whole_filled
FROM cleaned;
