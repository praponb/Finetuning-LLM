SELECT id, revenue, units FROM store_sales WHERE revenue < (SELECT MIN(revenue) FROM store_sales);
