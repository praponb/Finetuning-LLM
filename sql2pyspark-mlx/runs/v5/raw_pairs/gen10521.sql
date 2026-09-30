with valid as (
    -- Basic cleansing and defaults
    select
        *,
        lower(method) method_lower,
        datediff(current_date(), paid_at) days_since_paid_at,
        coalesce(method, 'N/A') resolved_method
    from payments
    where method in ('cash', 'bank_transfer')
)
-- Running and moving aggregates
select
    id,
    amount,
    paid_at,
    status,
    method_lower,
    days_since_paid_at,
    resolved_method,
    max(days_since_paid_at) over (partition by status order by amount, id rows between unbounded preceding and current row) running_max_days_since_paid_at,
    count(days_since_paid_at) over (partition by status order by amount, id rows between unbounded preceding and current row) running_count_days_since_paid_at,
    min(days_since_paid_at) over (partition by status order by amount, id rows between unbounded preceding and current row) running_min_days_since_paid_at,
    avg(days_since_paid_at) over (partition by status order by amount, id rows between unbounded preceding and current row) running_avg_days_since_paid_at
from valid
where amount < 850
