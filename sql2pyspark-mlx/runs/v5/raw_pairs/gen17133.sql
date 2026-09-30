SELECT s.id, s.product_id, s.revenue, s.sale_date FROM store_sales s WHERE s.revenue < (SELECT AVG(s2.revenue) FROM store_sales s2 WHERE s2.product_id = s.product_id)
