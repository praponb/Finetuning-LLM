SELECT id, account_type FROM accounts WHERE customer_id IN (33, 34) AND (opened_date >= '2024-05-01' OR status = 'CLOSED');
