with ranked as (
    select *, rank() over (partition by supplier_id, brand order by launch_date desc) as row_num
    from products
    qualify row_num = 1
)
select
    case when cost >= 670 then 'LARGE' else 'SMALL' end as cost_tier,
    date_trunc('quarter', launch_date) as quarter_start,
    round(avg(cost), 2) as avg_cost,
    sum(case when brand = 'Umbrella' then price else 0 end) as umbrella_price,
    sort_array(collect_set(category)) as category_list,
    round(avg(stock_qty), 2) as avg_stock_qty,
    grouping(case when cost >= 670 then 'LARGE' else 'SMALL' end) as grouping_level
from ranked
group by grouping sets ((case when cost >= 670 then 'LARGE' else 'SMALL' end, date_trunc('quarter', launch_date)), (case when cost >= 670 then 'LARGE' else 'SMALL' end));
