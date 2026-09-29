with with_dims as (
    -- Attach customers attributes
    select src.*, b.credit_limit
    from sessions src
    left join customers b
        on src.customer_id <=> b.id
)
select
    id,
    customer_id,
    started_at,
    credit_limit
from with_dims
order by credit_limit
