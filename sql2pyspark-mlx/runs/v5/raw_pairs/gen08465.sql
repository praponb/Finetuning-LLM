select year(sale_date) as sale_year, max(sale_date) as last_sale_date from store_sales group by year(sale_date) order by sale_year desc
