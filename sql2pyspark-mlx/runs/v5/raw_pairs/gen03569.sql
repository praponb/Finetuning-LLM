with latest as (
    select *
    from (
        select *, row_number() over (partition by method, status order by paid_at, id) as latest_rank
        from payments
    ) ranked
    where latest_rank = 1
)
select
    date_trunc('month', paid_at) as activity_month,
    case when amount >= 1540 then 'HIGH' else 'LOW' end as amount_tier,
    min(paid_at) as first_paid_at,
    count(distinct id) as distinct_ids,
    avg(amount) filter (where method = 'paypal') as paypal_avg_amount
from latest
group by date_trunc('month', paid_at), case when amount >= 1540 then 'HIGH' else 'LOW' end
having paypal_avg_amount < 1000000
order by amount_tier nulls first
