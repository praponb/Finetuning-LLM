SELECT product_id, COUNT(DISTINCT unit_price) AS unique_products, ROUND(AVG(quantity), 2) AS avg_quantity, SUM(unit_price * quantity) AS revenue FROM order_items GROUP BY 1;
