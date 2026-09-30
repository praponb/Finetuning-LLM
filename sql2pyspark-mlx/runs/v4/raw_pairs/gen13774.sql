with enriched as (
    select
        a.*,
        b.status as order_status,
        b.amount as order_amount
    from payments a
    left join orders b
        on a.order_id = b.id
)
select
    id,
    order_id,
    method,
    amount,
    order_status,
    order_amount
from enriched
order by id desc nulls last, method desc
