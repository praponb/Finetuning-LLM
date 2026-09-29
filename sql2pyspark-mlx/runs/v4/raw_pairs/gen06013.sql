with matched_keys as (
    select order_id
    from shipments
    except all
    select order_id
    from shipments
    where year(delivered_date) >= 2024
),
verified as (
    select s.*
    from shipments s
    join matched_keys k on s.order_id = k.order_id
)
select
    case when cost >= 77 then 'LARGE' else 'SMALL' end as cost_tier,
    date_trunc('quarter', shipped_date) as quarter_start,
    carrier,
    count(*) as cnt,
    count(*) filter (where status = 'LOST') as lost_rows,
    size(collect_set(carrier)) as num_carriers
from verified
group by case when cost >= 77 then 'LARGE' else 'SMALL' end, date_trunc('quarter', shipped_date), carrier
order by cnt
