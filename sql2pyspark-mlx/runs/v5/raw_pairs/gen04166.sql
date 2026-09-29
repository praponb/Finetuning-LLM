select id, sale_date from store_sales where sale_date = (select max(sale_date) from store_sales where revenue >= 1000)
