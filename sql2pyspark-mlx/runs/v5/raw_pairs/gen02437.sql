SELECT s.id, s.product_id, s.units, s.sale_date FROM store_sales s WHERE s.units < (SELECT AVG(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) ORDER BY s.sale_date;
