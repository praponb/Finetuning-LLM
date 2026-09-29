select product_id, avg(units) avg_units, round(stddev(revenue), 2) stddev_revenue
from store_sales
where units = 17 and year(sale_date) = 2023
group by product_id
order by product_id
limit 5;
