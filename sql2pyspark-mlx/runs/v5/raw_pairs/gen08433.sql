SELECT sale_year, stddev_revenue, avg_revenue
FROM (
    SELECT YEAR(sale_date) AS sale_year, ROUND(STDDEV(revenue), 2) AS stddev_revenue, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY YEAR(sale_date)
) AS x
WHERE avg_revenue < 1900
ORDER BY avg_revenue DESC, sale_year
LIMIT 5;
