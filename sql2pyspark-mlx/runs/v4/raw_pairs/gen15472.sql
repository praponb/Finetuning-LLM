SELECT id, opened_date FROM accounts WHERE balance IS NULL OR status = 'FROZEN' ORDER BY id DESC;
