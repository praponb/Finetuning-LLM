SELECT id, units, product_id, sale_date, CAST(revenue AS INT) AS revenue_int FROM store_sales ORDER BY units DESC
