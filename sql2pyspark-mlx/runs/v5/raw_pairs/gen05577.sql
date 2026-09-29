with valid as (
    select *, case when quantity >= 209 then 'High' when quantity >= 38 then 'Medium' else 'Low' end as quantity_band
    from inventory
    where last_restock >= date '2023-08-21'
      and store_id in (1, 4)
)
select
    s.id,
    s.store_id,
    s.quantity_band,
    p.store_name,
    p.opened_date
from valid s
left join stores p
    on s.store_id = p.id;
