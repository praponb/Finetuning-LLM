select case when units >= 32 then 'high' when units >= 8 then 'mid' else 'low' end as units_bucket, avg(units) as avg_units, max(revenue) - min(revenue) as revenue_range
from store_sales
where product_id in (2, 3, 6, 7) or sale_date between '2023-07-01' and '2023-08-01'
group by 1
order by avg_units desc;
