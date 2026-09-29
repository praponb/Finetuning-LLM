SELECT s.id, s.product_id, s.units, s.store_id FROM store_sales s WHERE s.units = (SELECT MIN(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id);
