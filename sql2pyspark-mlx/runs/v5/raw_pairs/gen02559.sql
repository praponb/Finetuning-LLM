with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from products s
    where exists (
            select 1
            from inventory ch
            where ch.product_id = s.id
              and ch.product_id in (1, 7)
        )
)
-- Aggregate with conditional metrics
select
    case when price >= 920 then 'HIGH' else 'LOW' end as price_tier,
    date_trunc('quarter', launch_date) as quarter_start,
    sort_array(collect_set(brand)) as brand_list,
    count_if(brand = 'Acme') as acme_events,
    percent_rank() over (partition by case when price >= 920 then 'HIGH' else 'LOW' end order by count_if(brand = 'Acme') desc) as acme_events_percent_rank
from kept
group by case when price >= 920 then 'HIGH' else 'LOW' end, date_trunc('quarter', launch_date)
order by quarter_start desc
