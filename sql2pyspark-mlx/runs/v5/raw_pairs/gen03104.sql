SELECT country, ROUND(STDDEV(rating), 2) stddev_rating, MAX(rating) - MIN(rating) rating_range
FROM suppliers
WHERE (country = 'SG' AND city != 'Bangkok') OR rating <= 2.5
GROUP BY country;
