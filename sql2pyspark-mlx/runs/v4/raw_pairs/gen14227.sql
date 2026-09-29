SELECT city, country, COUNT(*) customer_count FROM customers WHERE country = 'UK' OR credit_limit >= 12900 GROUP BY city, country;
