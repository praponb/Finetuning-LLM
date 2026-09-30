SELECT
    oi.id,
    oi.product_id,
    oi.unit_price,
    o.store_id,
    o.channel,
    p.product_name,
    p.stock_qty,
    LAST_DAY(p.launch_date) AS launch_date_month_end
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.shipping_fee >= 39
