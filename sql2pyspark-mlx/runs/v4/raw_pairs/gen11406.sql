WITH with_windows AS (
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
        LEAD(shipping_fee, 2, 0) OVER (ORDER BY shipping_fee, id) AS next_shipping_fee,
        CORR(shipping_fee, amount) OVER w_running AS corr_shipping_fee_amount,
        RANK() OVER (PARTITION BY status ORDER BY discount DESC NULLS LAST) AS discount_rank
    FROM orders
    WINDOW w_running AS (PARTITION BY store_id)
)
SELECT
    id,
    customer_id,
    store_id,
    status,
    channel,
    discount,
    shipping_fee,
    next_shipping_fee,
    corr_shipping_fee_amount
FROM with_windows
ORDER BY store_id DESC;
