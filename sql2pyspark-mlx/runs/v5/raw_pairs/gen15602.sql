SELECT id, product_id, revenue * units AS revenue_per_unit FROM store_sales WHERE revenue BETWEEN 1500 AND 2300 OR units < 11 ORDER BY product_id DESC, id DESC;
