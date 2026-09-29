SELECT *, COALESCE(revenue, 0) AS revenue_filled, revenue * units AS revenue_per_unit FROM store_sales WHERE units >= 14 ORDER BY product_id DESC;
