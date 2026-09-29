SELECT
    id,
    order_date,
    status,
    channel,
    discount,
    shipping_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS seq_no,
    CORR(amount, discount) OVER w_part AS corr_amount_discount
FROM orders
WINDOW w_part AS (PARTITION BY status)
