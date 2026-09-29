SELECT
    id,
    store_id,
    order_date,
    status,
    discount,
    LEAD(shipping_fee) OVER w_recent next_shipping_fee,
    LAG(shipping_fee) OVER w_recent prev_shipping_fee,
    LEAD(discount) OVER w_recent next_discount,
    LAG(discount) OVER w_recent prev_discount,
    ROW_NUMBER() OVER w_recent row_seq,
    NTILE(3) OVER (PARTITION BY channel ORDER BY shipping_fee, id) shipping_fee_bucket
FROM orders
WHERE status = 'CANCELLED'
WINDOW w_recent AS (PARTITION BY store_id ORDER BY shipping_fee, id);
