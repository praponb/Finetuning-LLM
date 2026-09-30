SELECT *
FROM (
    SELECT MONTH(sale_date) sale_month, COUNT(units) units_count
    FROM store_sales
    GROUP BY MONTH(sale_date)
) s
WHERE units_count < 9
ORDER BY units_count DESC, 1 DESC
