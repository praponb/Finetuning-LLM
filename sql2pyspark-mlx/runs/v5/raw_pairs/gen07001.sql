SELECT
    id,
    method,
    status,
    CASE WHEN method = 'card' THEN 1 ELSE 0 END AS is_card
FROM payments
