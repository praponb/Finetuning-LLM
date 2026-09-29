with base as (
    select *
    from payments
    where status = 'REFUNDED' and paid_at >= '2023-10-01'
),
summary as (
    select status, hour(paid_at) as paid_hour, avg(amount) as avg_amount
    from base
    group by status, hour(paid_at)
)
select *
from summary
order by status desc;
