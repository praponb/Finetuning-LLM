SELECT
    id,
    method,
    order_id,
    amount,
    AVG(amount) OVER (PARTITION BY method, order_id) AS method_avg_amount
FROM payments;
