select
    status,
    year(order_date) as order_year,
    min(order_date) as first_order_date,
    sum(case when channel = 'web' then discount else 0 end) as web_discount,
    sort_array(collect_set(status)) as status_list
from orders
where amount between 700 and 1090
group by rollup(status, year(order_date));
