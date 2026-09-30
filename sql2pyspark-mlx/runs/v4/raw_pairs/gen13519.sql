SELECT opened_date FROM accounts WHERE status = 'OPEN' UNION SELECT last_restock AS opened_date FROM inventory
