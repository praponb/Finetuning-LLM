SELECT signup_date FROM customers UNION SELECT last_restock AS signup_date FROM inventory WHERE quantity < 142 ORDER BY signup_date;
