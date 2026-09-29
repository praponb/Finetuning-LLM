with StoreSummary as (
    select store_id, min(quantity) min_quantity, sum(quantity) total_quantity
    from inventory
    group by store_id
)
select
    s.id,
    s.store_name,
    s.city,
    S.min_quantity,
    S.total_quantity
from stores s
left join StoreSummary S on s.id = S.store_id
order by city
