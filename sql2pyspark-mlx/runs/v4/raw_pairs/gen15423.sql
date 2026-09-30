select launch_month, max_cost
from (
    select date_format(launch_date, 'yyyy-MM') as launch_month, max(cost) as max_cost
    from products
    group by date_format(launch_date, 'yyyy-MM')
) as agg
where max_cost > 650
order by launch_month desc;
