SELECT c.country, c.city, a.opened_date, a.account_type FROM customers c LEFT JOIN accounts a ON a.customer_id = c.id;
