SELECT opened_date FROM stores WHERE region IN ('West', 'South', 'East') UNION ALL SELECT signup_date FROM customers
