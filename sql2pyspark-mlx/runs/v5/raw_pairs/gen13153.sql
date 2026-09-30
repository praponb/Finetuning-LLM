select *
from (
    select year(launch_date) as launch_year, avg(stock_qty) as avg_stock_qty, sort_array(collect_set(brand)) as brand_list
    from products
    group by year(launch_date)
) as s
where avg_stock_qty >= 170;
