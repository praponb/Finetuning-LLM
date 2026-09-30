SELECT a.id, a.account_id, a.amount FROM transactions a WHERE a.amount = (SELECT MIN(b.amount) FROM transactions b WHERE b.account_id = a.account_id) ORDER BY a.amount DESC;
