with RecentPayments as (
    select *
    from payments
    where order_id in (8, 22, 24) or status in ('SUCCESS', 'REFUNDED', 'FAILED')
),
Agg as (
    select status, min(paid_at) as first_paid_at, count(*) as row_count
    from RecentPayments
    group by status
)
select status, row_count
from Agg
where row_count > 10
order by row_count;
