SELECT
    id,
    status,
    balance,
    customer_id,
    CASE WHEN status IN ('OPEN', 'CLOSED') THEN 'Tier 1' ELSE 'Other' END AS status_group
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN') OR customer_id IN (6, 13, 24, 39)
