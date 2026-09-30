SELECT id, units, GREATEST(revenue, units) AS greatest_revenue_units FROM store_sales ORDER BY id DESC
