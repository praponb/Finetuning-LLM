with valid as (
    select *, coalesce(amount, 0) as amount_filled
    from payments
    where paid_at > date '2023-06-10'
      and method is not null
),
metrics as (
    select
        *,
        sum(amount) over (order by amount, id rows between unbounded preceding and current row) as running_amount,
        avg(amount_filled) over w_running as amount_filled_avg_6_prior,
        max(amount_filled) over w_part as method_max_amount_filled,
        sum(amount) over (partition by status order by amount_filled, id rows between unbounded preceding and current row) as cumulative_amount,
        ntile(5) over w_part_2 as amount_filled_bucket
    from valid
    where order_id is not null
    window w_running as (order by paid_at, id rows between 6 preceding and 1 preceding), w_part as (partition by method), w_part_2 as (partition by order_id, method order by amount_filled, id)
),
enriched as (
    select
        *,
        ntile(10) over (partition by method order by running_amount, id) as running_amount_bucket,
        avg(amount_filled_avg_6_prior) over (partition by order_id) as order_id_avg_amount_filled_avg_6_prior
    from metrics
),
ranked as (
    select
        id,
        order_id,
        status,
        running_amount,
        amount_filled_avg_6_prior,
        cumulative_amount,
        amount_filled_bucket,
        order_id_avg_amount_filled_avg_6_prior
    from enriched
    qualify row_number() over (partition by status, order_id order by amount_filled_bucket desc, id) = 1
)
select
    id,
    status,
    running_amount,
    amount_filled_avg_6_prior,
    cumulative_amount,
    last_value(amount_filled_avg_6_prior) ignore nulls over (partition by status order by cumulative_amount, id rows between unbounded preceding and current row) as last_known_amount_filled_avg_6_prior,
    lag(amount_filled_bucket, 2, 0) over (partition by order_id order by cumulative_amount, id) as prev_amount_filled_bucket
from ranked
