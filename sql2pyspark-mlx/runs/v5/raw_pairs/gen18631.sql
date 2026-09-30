SELECT store_id, COUNT(units) AS units_count
FROM store_sales
GROUP BY store_id
ORDER BY units_count DESC;
