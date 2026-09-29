WITH filtered AS (
    SELECT
        id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        WEEKOFYEAR(order_date) AS order_date_weekofyear
    FROM orders
    WHERE order_date > DATE '2023-06-13'
      AND store_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, channel ORDER BY discount, id) = 1
),
cleaned AS (
    SELECT
        id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_weekofyear,
        YEAR(order_date) AS order_date_year,
        UPPER(channel) AS channel_upper,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM latest
    WHERE order_date >= DATE '2023-12-29'
      AND channel IS NOT NULL
),
totals AS (
    SELECT
        CASE WHEN amount >= 1630 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(order_date_weekofyear) AS max_order_date_weekofyear,
        CORR(shipping_fee, discount) AS corr_shipping_fee_discount
    FROM cleaned
    GROUP BY 1
)
SELECT amount_tier, max_order_date_weekofyear
FROM totals
ORDER BY amount_tier NULLS LAST, max_order_date_weekofyear;
