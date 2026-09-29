with recent_payments as (
    select *
    from payments
    where amount between 410 and 520 and paid_at < '2024-10-16'
),
summary as (
    select status, sum(case when method = 'card' then 1 else 0 end) as card_count, min(amount) as min_amount
    from recent_payments
    group by status
)
select status, card_count, min_amount
from summary
order by card_count
