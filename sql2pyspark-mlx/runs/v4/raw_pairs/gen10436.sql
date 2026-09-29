SELECT id, account_type, CASE WHEN status IN ('OPEN', 'CLOSED') THEN 'Tier 1' ELSE 'Other' END AS status_group
FROM accounts
WHERE account_type IN ('CREDIT', 'SAVINGS')
