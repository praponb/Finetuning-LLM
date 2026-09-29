with valid_payments as (
    select *
    from payments
    where order_id = 29 and amount > 690
),
summary as (
    select
        method,
        case when amount >= 1410 then 'high' when amount >= 600 then 'mid' else 'low' end as amount_bucket,
        count(*) as payment_count,
        count(amount) as amount_count
    from valid_payments
    group by method, case when amount >= 1410 then 'high' when amount >= 600 then 'mid' else 'low' end
)
select *
from summary;
