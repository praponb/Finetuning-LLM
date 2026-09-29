SELECT *
FROM (
    SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    GROUP BY store_id
) AS x
WHERE revenue_range >= 80
ORDER BY store_id DESC
