SELECT id, units FROM store_sales WHERE units < (SELECT MIN(units) FROM store_sales WHERE sale_date <= '2025-10-01') ORDER BY units DESC, id DESC;
