SELECT a.id, a.status, a.account_type FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions) ORDER BY a.status ASC, id ASC LIMIT 3;
