SELECT
    *,
    LAG(discount, 1) OVER (PARTITION BY store_id, status ORDER BY amount, id) AS prev_discount,
    LEAD(shipping_fee, 1) OVER (PARTITION BY channel ORDER BY order_date, id) AS next_shipping_fee,
    NTILE(5) OVER (PARTITION BY status ORDER BY amount, id) AS amount_bucket
FROM orders
