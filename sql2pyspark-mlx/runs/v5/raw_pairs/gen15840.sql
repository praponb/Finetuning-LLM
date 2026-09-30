with cleaned as (
    select
        *,
        datediff(shipped_date, delivered_date) as days_delivered_date_to_shipped_date,
        coalesce(carrier, 'N/A') as resolved_carrier
    from shipments
    where carrier is not null
      and carrier is distinct from 'Flash'
),
filtered as (
    select
        *,
        coalesce(days_delivered_date_to_shipped_date, 0) as days_delivered_date_to_shipped_date_filled,
        round(days_delivered_date_to_shipped_date, 2) as days_delivered_date_to_shipped_date_rounded
    from cleaned
    where carrier is not null
      and delivered_date between '2023-04-27' and '2023-08-31'
),
latest as (
    select *
    from (
        select *, row_number() over (partition by carrier order by days_delivered_date_to_shipped_date desc, id asc) as row_num
        from filtered
    ) t
    where row_num = 1
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by order_id, status order by days_delivered_date_to_shipped_date_rounded desc nulls first, id asc) as latest_rank
        from latest
    ) ranked
    where latest_rank = 1
),
summary as (
    select
        date_trunc('year', delivered_date) as year_start,
        round(avg(days_delivered_date_to_shipped_date_rounded), 2) as avg_days_delivered_date_to_shipped_date_rounded,
        sort_array(collect_set(status)) as status_list,
        count(distinct carrier) as distinct_carriers
    from ranked
    group by 1
)
select
    year_start,
    status_list,
    distinct_carriers
from summary
order by year_start asc nulls last
limit 20;
