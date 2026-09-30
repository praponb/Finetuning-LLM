SELECT location FROM departments UNION SELECT city FROM suppliers WHERE country NOT IN ('UK', 'DE', 'SG') ORDER BY location
