SELECT
    id,
    opened_date,
    customer_id,
    LOWER(status) AS status_lower
FROM accounts
WHERE account_type != 'CHECKING' AND YEAR(opened_date) >= 2025
ORDER BY status_lower;
