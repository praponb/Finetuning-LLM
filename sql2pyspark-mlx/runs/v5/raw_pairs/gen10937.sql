with filtered as (
    select *, date_format(paid_at, 'yyyy-MM') as paid_at_label
    from payments
    where paid_at >= to_timestamp('2023-06-03 14:00:00')
      and method is distinct from 'cash'
),
cleaned as (
    select
        *,
        lower(method) as method_lower,
        lower(method) as method_lower_2
    from filtered
    where method is not null
),
enriched as (
    select
        *,
        lead(amount, 2, 0) over (partition by method order by amount, id) as next_amount,
        first_value(method, true) over (partition by method order by amount, id rows between unbounded preceding and current row) as first_known_method,
        rank() over (partition by status order by amount desc) as amount_rank,
        min(amount) over (partition by status) as status_min_amount,
        row_number() over (partition by status, order_id order by paid_at desc, id) as seq_no
    from cleaned
),
with_prev as (
    select *, lag(paid_at) over (partition by order_id order by paid_at, id) as prev_paid_at
    from enriched
),
flagged as (
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '7' day then 1 else 0 end as is_new_session
    from with_prev
),
sessions_payments as (
    select *, sum(is_new_session) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    o.status as order_status,
    src.method,
    count(*) as cnt,
    sum(src.amount_rank) as total_amount_rank,
    grouping_id() as level_id
from sessions_payments src
inner join orders o on src.order_id = o.id
group by grouping sets ((o.status, src.method), (o.status), ());
