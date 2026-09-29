SELECT opened_date FROM accounts WHERE customer_id IS NULL UNION SELECT shipped_date FROM shipments;
