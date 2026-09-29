with recent_payments as (
    select *
    from payments
    where method in ('promptpay', 'bank_transfer', 'paypal') and (order_id in (5, 28) or method is not null)
),
agg as (
    select status, max(amount) - min(amount) as amount_range, min(amount) as min_amount
    from recent_payments
    group by status
)
select status, amount_range, min_amount
from agg
where min_amount < 1380
order by status
limit 5;
