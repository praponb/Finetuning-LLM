with valid as (
    select
        *,
        datediff(current_date(), last_restock) as days_since_last_restock,
        date_format(last_restock, 'yyyy-MM') as last_restock_label
    from inventory
    where last_restock >= date '2023-02-18'
),
filtered as (
    select *, coalesce(quantity, 0) as quantity_filled
    from valid
    where last_restock >= date '2023-07-18'
      and store_id in (2, 5)
),
with_dims as (
    select
        a.id,
        a.store_id,
        a.days_since_last_restock,
        a.last_restock_label,
        a.quantity_filled,
        p.opened_date,
        p.city,
        p.store_name
    from filtered a
    left join stores p
        on a.store_id = p.id
),
enriched as (
    select
        *,
        row_number() over w_grp as row_seq,
        avg(days_since_last_restock) over w_recent as days_since_last_restock_avg_2
    from with_dims
    window w_grp as (partition by city order by opened_date desc, id), w_recent as (partition by city order by days_since_last_restock, id rows between 2 preceding and current row)
)
select
    id,
    store_id,
    quantity_filled,
    opened_date,
    city,
    row_seq,
    days_since_last_restock_avg_2,
    case when city rlike '^(Bangkok|Tokyo)$' then 'EXTENDED' when city rlike '^(Chiang Mai|New York)$' then 'GROUP_A' else 'PRIORITY' end as city_zone
from enriched
order by row_seq;
