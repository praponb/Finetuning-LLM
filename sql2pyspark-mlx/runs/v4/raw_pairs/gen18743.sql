SELECT
    o.id,
    o.order_date,
    s.opened_date,
    c.city,
    o.discount - o.amount discount_minus_amount
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE s.store_name IS NOT NULL
ORDER BY order_date DESC
