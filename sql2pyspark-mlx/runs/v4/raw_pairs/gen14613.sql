SELECT opened_date FROM stores UNION SELECT last_restock AS opened_date FROM inventory;
