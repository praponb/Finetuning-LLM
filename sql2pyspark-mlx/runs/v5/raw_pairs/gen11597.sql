with filtered_payments as (
    select *
    from payments
    where method = 'bank_transfer'
),
PaymentsStats as (
    select order_id, round(avg(amount), 2) as avg_amount
    from filtered_payments
    group by order_id
)
select *
from PaymentsStats
where avg_amount > 34
order by order_id, avg_amount
limit 10
