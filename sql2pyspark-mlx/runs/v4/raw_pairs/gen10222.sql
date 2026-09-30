SELECT
    status,
    opened_date,
    account_type,
    CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END is_closed,
    LOWER(status) status_lower
FROM accounts
WHERE opened_date < '2023-11-15' AND account_type = 'CHECKING'
