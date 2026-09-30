SELECT store_id, MAX(revenue) AS max_revenue
FROM store_sales
GROUP BY store_id
ORDER BY store_id DESC;
