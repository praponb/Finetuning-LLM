with filtered_payments as (
    select *
    from payments
    where to_date(paid_at) >= '2024-02-01' or status in ('REFUNDED', 'FAILED')
),
payments_stats as (
    select method, status, round((100 * sum(case when status = 'REFUNDED' then 1 else 0 end)) / count(*), 1) as pct_refunded
    from filtered_payments
    group by method, status
)
select method, status
from payments_stats
where pct_refunded < 61
order by method desc;
