SELECT segment, signup_month, unique_segments
FROM (
    SELECT segment, DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, COUNT(DISTINCT age) AS unique_segments
    FROM customers
    GROUP BY segment, DATE_FORMAT(signup_date, 'yyyy-MM')
) AS sub
WHERE unique_segments < 8
ORDER BY unique_segments DESC;
