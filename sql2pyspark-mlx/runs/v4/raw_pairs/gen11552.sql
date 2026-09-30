SELECT
    src.*,
    a.account_type,
    a.opened_date,
    a.status
FROM transactions src
JOIN accounts a
    ON src.account_id = a.id
