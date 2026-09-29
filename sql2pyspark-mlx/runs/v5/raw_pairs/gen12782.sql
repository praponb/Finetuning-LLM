with with_latest as (
    select
        s.id,
        s.city,
        latest.inventory_quantity,
        latest.last_restock
    from stores s
    left join lateral (
            select c.quantity as inventory_quantity, c.last_restock
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    select
        *,
        last_value(inventory_quantity, true) over (order by inventory_quantity, id rows between unbounded preceding and current row) as last_known_inventory_quantity,
        last_value(inventory_quantity, true) over (partition by city order by last_restock, id rows between unbounded preceding and current row) as last_known_inventory_quantity_2
    from with_latest
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by city order by inventory_quantity desc, id) as rank_in_group
        from with_windows
    ) ranked
    where rank_in_group = 1
),
base as (
    select
        *,
        case when last_known_inventory_quantity_2 >= 208 then 'High' when last_known_inventory_quantity_2 >= 69 then 'Medium' else 'Low' end as last_known_inventory_quantity_2_band,
        dayofweek(last_restock) as last_restock_dayofweek
    from deduped
    where last_restock >= '2024-11-27'
),
latest_match as (
    select s.*, top1.order_date
    from base s
    left join lateral (
            select c.order_date
            from orders c
            where c.store_id = s.id
            order by c.discount desc, c.id desc
            limit 1
        ) top1 on true
)
select
    city,
    round(avg(last_known_inventory_quantity), 2) as avg_last_known_inventory_quantity,
    sum(last_restock_dayofweek) filter (where city = 'Berlin') as berlin_total_last_restock_dayofweek
from latest_match
group by city
order by avg_last_known_inventory_quantity desc nulls last
