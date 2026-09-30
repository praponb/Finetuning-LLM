SELECT s.id, s.product_id, s.units FROM store_sales s WHERE s.units = (SELECT MAX(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id);
