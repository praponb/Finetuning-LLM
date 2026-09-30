with enriched as (
    -- Attach stores attributes
    select
        e.*,
        s.city,
        s.opened_date
    from inventory e
    left join stores s
        on e.store_id = s.id
)
select
    id,
    store_id,
    product_id,
    quantity
from enriched
order by store_id, id
