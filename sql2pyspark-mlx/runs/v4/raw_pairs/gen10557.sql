SELECT
    e.*,
    b.balance,
    b.account_type
FROM transactions e
LEFT JOIN accounts b
    ON e.account_id = b.id
