SELECT
    id,
    order_date,
    shipping_fee,
    store_id
FROM orders
WHERE channel IN ('store', 'partner', 'mobile')
ORDER BY order_date ASC, store_id DESC
