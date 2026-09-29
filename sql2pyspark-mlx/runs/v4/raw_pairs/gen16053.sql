select year(sale_date) as sale_year, date_trunc('MONTH', sale_date) as sale_month, sum(units) as total_units
from store_sales
group by year(sale_date), date_trunc('MONTH', sale_date)
order by total_units desc;
