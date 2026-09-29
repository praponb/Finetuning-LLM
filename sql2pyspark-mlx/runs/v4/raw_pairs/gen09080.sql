SELECT opened_date AS event_date
FROM accounts
WHERE customer_id IS NULL
EXCEPT
SELECT shipped_date AS event_date
FROM shipments
