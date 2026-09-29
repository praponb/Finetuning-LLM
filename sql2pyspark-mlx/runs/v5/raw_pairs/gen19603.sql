SELECT country, pct_sg, us_count
FROM (
    SELECT country, ROUND((100 * SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sg, SUM(CASE WHEN country = 'US' THEN 1 ELSE 0 END) AS us_count
    FROM suppliers
    GROUP BY country
) AS t
WHERE us_count > 56;
