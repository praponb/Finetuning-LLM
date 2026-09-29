SELECT id, opened_date FROM accounts WHERE balance > 24000 UNION SELECT id, opened_date FROM accounts WHERE status = 'CLOSED';
