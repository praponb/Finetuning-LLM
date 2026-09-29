SELECT s.sqft, t.first_sale_date FROM stores s INNER JOIN (SELECT store_id, MIN(sale_date) AS first_sale_date FROM store_sales GROUP BY store_id) t ON s.id = t.store_id
