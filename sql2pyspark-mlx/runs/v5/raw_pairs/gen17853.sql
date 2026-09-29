SELECT
    id,
    status,
    amount,
    MIN(amount) OVER (PARTITION BY status) AS status_min_amount,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS amount_row
FROM payments
ORDER BY id DESC;
