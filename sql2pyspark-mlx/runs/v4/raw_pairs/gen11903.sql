SELECT a.id, a.customer_id, a.opened_date, a.balance, c.segment FROM accounts a RIGHT OUTER JOIN customers c ON c.id = a.customer_id ORDER BY customer_id DESC
