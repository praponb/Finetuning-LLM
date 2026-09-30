SELECT
    id,
    order_id,
    amount,
    status,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS amount_row,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS amount_row_2
FROM payments
ORDER BY amount_row;
