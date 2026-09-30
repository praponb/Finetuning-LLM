with scored as (
    select
        *,
        round(amount / sum(amount) over (partition by status), 4) as amount_share,
        lag(amount, 1) over (partition by method order by amount, id) as prev_amount
    from payments
),
metrics as (
    select
        *,
        percent_rank() over w_part as amount_percent_rank,
        sum(amount) over w_ordered as running_amount,
        cume_dist() over w_grp as amount_share_cume_dist,
        last_value(method) ignore nulls over w_grp_2 as last_known_method,
        sum(amount) over (order by paid_at, id rows between unbounded preceding and current row) as cumulative_amount
    from scored
    window w_part as (partition by order_id order by amount), w_ordered as (partition by method, order_id order by paid_at, id rows between unbounded preceding and current row), w_grp as (partition by order_id order by amount_share desc), w_grp_2 as (partition by status order by paid_at, id rows between unbounded preceding and current row)
),
filtered as (
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        prev_amount,
        amount_percent_rank,
        amount_share_cume_dist,
        last_known_method,
        round(cumulative_amount, 0) as cumulative_amount_rounded
    from metrics
    where paid_at > to_date('2023-08-14')
),
metrics_payments as (
    select
        *,
        lead(amount_percent_rank, 2) over (partition by method order by cumulative_amount_rounded, id) as next_amount_percent_rank,
        percent_rank() over (partition by status order by amount_share_cume_dist nulls first) as amount_share_cume_dist_percent_rank,
        row_number() over (partition by method order by amount_percent_rank, id) as row_seq
    from filtered
),
matched_keys as (
    select status, method
    from metrics_payments
    intersect all
    select status, method
    from payments
    where to_date(paid_at) < '2024-10-26'
),
kept_rows as (
    select s.*
    from metrics_payments s
    inner join matched_keys k on s.status = k.status and s.method = k.method
),
with_dims as (
    select s.*, o.status as order_status
    from kept_rows s
    inner join orders o
        on s.order_id = o.id
)
select
    id,
    method,
    paid_at,
    status,
    prev_amount,
    amount_share_cume_dist,
    last_known_method,
    cumulative_amount_rounded,
    next_amount_percent_rank,
    case when method rlike '^(bank_transfer|promptpay)$' then 'CORE' when method rlike '^(card|paypal)$' then 'GROUP_B' else 'GROUP_A' end as method_zone
from with_dims
order by id desc nulls first
