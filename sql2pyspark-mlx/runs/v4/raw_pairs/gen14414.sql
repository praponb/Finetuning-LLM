SELECT
    customer_id,
    status,
    CAST(discount AS INT) AS discount_int,
    ROUND(amount, 2) AS amount_rounded
FROM orders
WHERE YEAR(order_date) = 2023;
