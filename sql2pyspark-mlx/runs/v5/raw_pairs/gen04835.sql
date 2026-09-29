with store_summary as (
    select store_id, round(avg(quantity), 2) as avg_quantity, max(quantity) as max_quantity
    from inventory
    group by store_id
)
select
    s.city,
    s.opened_date,
    ss.avg_quantity,
    ss.max_quantity
from stores s
join store_summary ss on s.id = ss.store_id
order by ss.avg_quantity desc
