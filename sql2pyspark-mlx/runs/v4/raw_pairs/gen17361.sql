SELECT city, country FROM suppliers WHERE rating <= 2.4 UNION ALL SELECT city, country FROM suppliers WHERE country = 'US' ORDER BY city
