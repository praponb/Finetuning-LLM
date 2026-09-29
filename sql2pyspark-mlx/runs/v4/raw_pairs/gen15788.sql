SELECT opened_month, stddev_sqft, sqft_range
FROM (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, ROUND(STDDEV(sqft), 2) AS stddev_sqft, MAX(sqft) - MIN(sqft) AS sqft_range
    FROM stores
    WHERE opened_date > '2025-02-01'
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
) AS s
WHERE stddev_sqft > 45;
