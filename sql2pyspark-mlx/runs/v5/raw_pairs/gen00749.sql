SELECT ss.product_id, p.cost, p.product_name, QUARTER(ss.sale_date) AS sale_date_quarter FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE ss.product_id IN (4, 5);
