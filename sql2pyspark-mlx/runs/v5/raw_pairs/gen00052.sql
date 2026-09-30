SELECT
    payments.status,
    orders.amount,
    customers.country,
    customers.is_active,
    LAST_DAY(orders.order_date) order_date_month_end
FROM payments
FULL OUTER JOIN orders ON payments.order_id = orders.id
INNER JOIN customers ON customers.id = orders.customer_id
WHERE orders.amount <= 660
