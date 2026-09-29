select year(sale_date) as sale_year, min(sale_date) as min_sale_date
from store_sales
where store_id in (2, 4) and product_id = 1 and sale_date >= '2023-11-01'
group by sale_year
order by min_sale_date asc;
