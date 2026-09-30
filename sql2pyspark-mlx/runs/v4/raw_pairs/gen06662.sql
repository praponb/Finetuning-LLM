with deduped as (
    -- Keep one row per key (latest first)
    select *
    from inventory
    qualify row_number() over (partition by product_id order by quantity desc nulls first, id) = 1
)
select id, quantity
from deduped;
