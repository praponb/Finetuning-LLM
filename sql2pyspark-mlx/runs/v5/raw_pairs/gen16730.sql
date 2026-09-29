select sale_month, stddev_revenue
from (
    select month(sale_date) sale_month, round(stddev(revenue), 2) stddev_revenue
    from store_sales
    group by month(sale_date)
) t
where stddev_revenue > 20
order by stddev_revenue desc, sale_month desc;
