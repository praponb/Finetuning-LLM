with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        amount,
        paid_at,
        status,
        substring(method, 1, 3) as method_prefix
    from payments
    where paid_at >= date '2023-11-14'
      and order_id is null
)
select status, id
from cleaned
intersect all
select status, id
from payments
where status in ('REFUNDED', 'FAILED');
