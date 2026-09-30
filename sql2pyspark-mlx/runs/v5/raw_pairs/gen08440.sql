SELECT t.id, t.is_flagged, a.status FROM transactions t INNER JOIN accounts a ON a.id = t.account_id ORDER BY id ASC LIMIT 50
