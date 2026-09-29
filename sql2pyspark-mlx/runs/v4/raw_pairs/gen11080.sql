SELECT a.id, a.store_id, a.revenue, a.sale_date FROM store_sales a WHERE a.revenue > (SELECT AVG(b.revenue) FROM store_sales b WHERE b.store_id = a.store_id);
