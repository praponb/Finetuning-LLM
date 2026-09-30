with with_windows as (
    select
        *,
        avg(amount) over (partition by method order by paid_at, id rows between 6 preceding and current row) last7_avg_amount,
        max(amount) over (partition by method order by paid_at, id rows between 6 preceding and current row) last7_max_amount,
        count(amount) over (partition by method order by paid_at, id rows between 6 preceding and current row) last7_count_amount,
        min(amount) over (partition by method order by paid_at, id rows between 6 preceding and current row) last7_min_amount,
        lead(amount, 2) over (partition by status, order_id order by paid_at, id) next_amount
    from payments
)
select
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    last7_avg_amount,
    last7_max_amount,
    last7_count_amount,
    case when status rlike '^(REFUNDED|SUCCESS)$' then 'OTHER_ZONE' else 'CORE' end as status_zone
from with_windows
order by amount
