SELECT
    DATE_TRUNC('MONTH', c.signup_date) signup_month,
    c.country,
    ROUND(AVG(o.discount), 2) avg_discount,
    MAX(o.shipping_fee) max_shipping_fee
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY DATE_TRUNC('MONTH', c.signup_date), c.country
ORDER BY max_shipping_fee DESC
