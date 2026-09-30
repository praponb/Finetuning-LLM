SELECT merchant, AVG(amount) AS avg_amount, MAX(amount) AS max_amount, COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count FROM transactions GROUP BY merchant HAVING amazon_count > 3
