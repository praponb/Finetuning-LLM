SELECT *
FROM (
    SELECT MONTH(opened_date) AS opened_month, QUARTER(opened_date) AS opened_quarter, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM stores
    WHERE city = 'London'
    GROUP BY MONTH(opened_date), QUARTER(opened_date)
) AS sub
WHERE stddev_sqft >= 26
ORDER BY stddev_sqft ASC;
