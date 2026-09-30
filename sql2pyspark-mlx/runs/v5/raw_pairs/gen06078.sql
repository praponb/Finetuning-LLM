select *
from (
    select year(launch_date) as launch_year, min(launch_date) as min_launch_date, count(case when category = 'Electronics' then 1 end) as electronics_count
    from products
    group by year(launch_date)
) as agg
where electronics_count >= 8
order by electronics_count desc
