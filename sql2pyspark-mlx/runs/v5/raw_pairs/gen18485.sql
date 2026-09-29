with supplier_summary as (
    select supplier_id, round(stddev(cost), 2) as stddev_cost, concat_ws(', ', sort_array(collect_set(category))) as category_values
    from products
    where launch_date between '2023-08-01' and '2023-12-01'
    group by supplier_id
)
select
    s.id,
    s.supplier_name,
    ss.stddev_cost,
    ss.category_values
from suppliers s
left join supplier_summary ss on s.id = ss.supplier_id
where ss.stddev_cost >= 30
order by s.id;
