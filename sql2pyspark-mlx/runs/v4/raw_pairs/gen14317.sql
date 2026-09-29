with store_summary as (
    select store_id, count(units) as units_count, min(units) as min_units
    from store_sales
    where store_id in (3, 4, 5, 6)
    group by store_id
)
select
    s.id,
    s.store_name,
    ss.units_count,
    ss.min_units
from stores s
left join store_summary ss on s.id = ss.store_id
order by s.store_name
