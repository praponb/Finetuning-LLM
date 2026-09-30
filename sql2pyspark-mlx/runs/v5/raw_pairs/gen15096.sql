SELECT 'account' AS kind, opened_date FROM accounts UNION SELECT 'inventory' AS kind, last_restock FROM inventory WHERE quantity > 179
