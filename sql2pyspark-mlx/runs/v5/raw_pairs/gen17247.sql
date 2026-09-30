select product_id, year(sale_date) as sale_year, avg(revenue) as avg_revenue
from store_sales
group by product_id, year(sale_date);
