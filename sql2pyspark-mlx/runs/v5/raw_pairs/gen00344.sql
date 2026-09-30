SELECT
    id,
    status,
    amount,
    order_id,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) amount_share,
    NTILE(4) OVER (PARTITION BY order_id ORDER BY amount DESC, id) amount_ntile
FROM payments
ORDER BY amount_ntile DESC;
