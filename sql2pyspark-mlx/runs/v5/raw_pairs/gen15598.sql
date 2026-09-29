SELECT id, revenue, store_id FROM store_sales WHERE revenue >= (SELECT AVG(revenue) FROM store_sales);
