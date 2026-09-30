SELECT
    customers.email,
    customers.credit_limit,
    customers.country,
    orders.order_date,
    orders.shipping_fee
FROM customers
LEFT JOIN orders ON customers.id = orders.customer_id
WHERE customers.country IN ('JP', 'UK', 'TH') AND customers.country <> 'UK';
