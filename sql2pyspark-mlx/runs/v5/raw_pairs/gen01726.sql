SELECT
    id,
    opened_date,
    account_type,
    status,
    COALESCE(status, 'unassigned') AS status_filled
FROM accounts
WHERE customer_id IN (13, 19, 21, 30) AND (customer_id IS NOT NULL OR status IS NOT NULL);
