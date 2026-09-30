with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('-', method, status) as method_status_key,
        coalesce(method, 'DIRECT') as method_filled
    from payments
    where order_id is not null
)
-- Deduplicate with ROW_NUMBER
select *, row_number() over (partition by order_id order by paid_at desc nulls last, id) as dedup_rank
from cleaned
qualify dedup_rank = 1;
