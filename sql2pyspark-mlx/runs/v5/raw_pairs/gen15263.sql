with by_dimension as (
    -- Join a dimension and aggregate
    select
        dim.status as order_status,
        src.status as payment_status,
        count(*) as rows_total,
        max(src.amount) as max_amount
    from payments src
    inner join orders dim on src.order_id = dim.id
    group by dim.status, src.status
)
select
    order_status,
    payment_status,
    max_amount
from by_dimension
order by order_status, max_amount
