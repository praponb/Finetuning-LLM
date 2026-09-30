with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        round(amount, 2) as amount_rounded,
        date_trunc('week', paid_at) as paid_at_period,
        coalesce(method, 'NONE') as resolved_method
    from payments
    where paid_at > timestamp '2023-10-20 17:00:00'
      and method is not null
)
-- Running and moving aggregates
select
    id,
    order_id,
    amount,
    paid_at,
    amount_rounded,
    paid_at_period,
    resolved_method,
    lag(amount, 1) over (partition by method order by amount_rounded asc, id asc) as prev_amount,
    avg(amount_rounded) over w_hist as amount_rounded_avg_2_prior
from filtered
where order_id = 24
window w_hist as (partition by order_id order by amount_rounded asc, id asc rows between 2 preceding and 1 preceding);
