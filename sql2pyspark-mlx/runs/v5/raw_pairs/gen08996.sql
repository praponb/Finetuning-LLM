with store_stats as (
    select store_id, count(distinct quantity) as unique_stores, max(quantity) as max_quantity
    from inventory
    group by store_id
)
select
    s.id,
    s.store_name,
    s.city,
    ss.unique_stores,
    ss.max_quantity
from stores s
left join store_stats ss on s.id = ss.store_id
order by s.id desc;
