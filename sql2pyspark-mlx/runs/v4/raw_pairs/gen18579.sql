SELECT id, rating, country, ROUND(rating * 1.07, 2) AS adjusted_rating FROM suppliers WHERE country NOT IN ('DE', 'TH', 'US') AND rating IS NULL AND supplier_name LIKE 'E%' ORDER BY 1
