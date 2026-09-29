SELECT
    id,
    name,
    segment,
    COALESCE(email, 'unassigned') AS email_filled
FROM customers
WHERE segment = 'SMB';
