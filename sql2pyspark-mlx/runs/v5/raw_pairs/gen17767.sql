SELECT
    id,
    order_id,
    amount,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS amount_row
FROM payments
