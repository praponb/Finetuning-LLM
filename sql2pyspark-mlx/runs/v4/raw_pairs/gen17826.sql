SELECT t.id, t.is_flagged, a.opened_date
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
