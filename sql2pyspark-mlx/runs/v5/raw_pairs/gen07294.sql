select sale_year, max_units from (select year(sale_date) as sale_year, max(units) as max_units from store_sales group by year(sale_date)) as sub where max_units < 18;
