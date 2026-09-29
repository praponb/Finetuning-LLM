WITH base AS (
    -- Filter the raw rows and derive helper columns
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
        ROUND(shipping_fee, 1) AS shipping_fee_rounded,
        DATE_FORMAT(order_date, 'yyyy-MM') AS order_date_label
    FROM orders
    WHERE channel IN ('mobile', 'web', 'store')
)
-- Non-equi join on a validity window
SELECT
    a.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM base a
INNER JOIN subscriptions sub
    ON a.customer_id = sub.customer_id
   AND a.order_date BETWEEN sub.start_date AND sub.end_date;
