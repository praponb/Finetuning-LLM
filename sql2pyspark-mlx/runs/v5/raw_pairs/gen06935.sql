with deduped as (
    -- QUALIFY-style window filter
    select *
    from shipments
    qualify row_number() over (partition by order_id order by cost desc, id) = 1
)
select
    id,
    order_id,
    carrier,
    cost,
    status
from deduped;
