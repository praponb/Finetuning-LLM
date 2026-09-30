SELECT city, SUM(rating) total_rating, SUM(SUM(rating)) OVER (ORDER BY city) AS cumulative_rating
FROM suppliers
WHERE country <> 'DE'
GROUP BY city
ORDER BY total_rating DESC, cumulative_rating DESC, city
LIMIT 5;
