SELECT city, COUNT(DISTINCT rating) unique_citys, MAX(rating) - MIN(rating) rating_range FROM suppliers WHERE country NOT IN ('UK', 'DE', 'US') AND city != 'Bangkok' GROUP BY 1 ORDER BY rating_range;
