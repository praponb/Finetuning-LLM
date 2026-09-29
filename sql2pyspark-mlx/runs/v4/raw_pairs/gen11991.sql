SELECT
    id,
    customer_id,
    discount,
    DATE_TRUNC('MONTH', order_date) order_date_period,
    COALESCE(discount, 0) discount_filled
FROM orders
WHERE MONTH(order_date) IN (8, 10, 12)
ORDER BY discount_filled;
