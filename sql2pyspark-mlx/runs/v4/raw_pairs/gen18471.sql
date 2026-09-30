SELECT s.*, latest.account_type FROM customers s JOIN LATERAL (SELECT c.account_type FROM accounts c WHERE c.customer_id = s.id ORDER BY c.opened_date DESC, c.id DESC LIMIT 1) latest ON TRUE
