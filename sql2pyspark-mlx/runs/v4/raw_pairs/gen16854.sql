SELECT a.id, a.customer_id, a.opened_date, c.city, c.age FROM accounts a JOIN customers c ON c.id = a.customer_id WHERE c.segment = 'SMB' AND a.opened_date <= '2023-07-01' ORDER BY id DESC
