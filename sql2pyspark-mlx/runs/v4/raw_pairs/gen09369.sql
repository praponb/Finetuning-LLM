SELECT store_id, COUNT(DISTINCT revenue) unique_products
FROM store_sales
GROUP BY 1;
