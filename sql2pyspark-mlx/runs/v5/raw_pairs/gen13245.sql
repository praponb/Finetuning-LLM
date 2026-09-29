with src as (
    select *
    from payments
    where amount >= 1200
),
paid_month_summary as (
    select date_format(paid_at, 'yyyy-MM') as paid_month, count(*) as num_payments
    from src
    group by date_format(paid_at, 'yyyy-MM')
)
select paid_month, num_payments
from paid_month_summary
where num_payments >= 7
order by paid_month
limit 10;
