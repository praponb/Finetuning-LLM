WITH src AS (
    SELECT *
    FROM store_sales
    WHERE revenue < 2400
),
store_sales_stats AS (
    SELECT store_id, YEAR(sale_date) AS sale_year, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM src
    GROUP BY store_id, YEAR(sale_date)
)
SELECT store_id, sale_year, stddev_revenue
FROM store_sales_stats
ORDER BY sale_year DESC
