SELECT *, FLOOR(units) AS units_whole, COALESCE(revenue, 0) AS revenue_filled FROM store_sales ORDER BY store_id DESC;
