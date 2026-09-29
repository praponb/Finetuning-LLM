with base as (
    select
        id,
        order_id,
        method,
        amount,
        status,
        coalesce(amount, 0) as amount_filled,
        lower(method) as method_lower,
        coalesce(method, 'DIRECT') as method_filled
    from payments
    where amount <= 1350
      and method is not null
),
metrics as (
    select
        id,
        order_id,
        amount,
        status,
        amount_filled,
        method_lower,
        method_filled,
        lag(amount) over (partition by status order by amount, id) as prev_amount,
        lag(amount_filled) over (partition by status order by amount, id) as prev_amount_filled,
        lead(amount_filled) over (partition by status order by amount, id) as next_amount_filled,
        row_number() over (partition by status order by amount, id) as row_seq,
        last_value(amount) ignore nulls over (partition by order_id order by amount, id rows between unbounded preceding and current row) as last_known_amount
    from base
    where amount_filled <= 52
),
enriched as (
    select s.*, o.order_date
    from metrics s
    left join orders o
        on s.order_id = o.id
),
deduped as (
    select *, row_number() over (partition by order_id, status order by amount desc nulls first, id) as rank_in_group
    from enriched
    qualify rank_in_group = 1
)
select
    d.channel,
    s.status as payment_status,
    count(*) as num_rows,
    sum(s.prev_amount) as total_prev_amount
from deduped s
left join orders d on s.order_id = d.id
group by cube (d.channel, s.status)
order by channel desc nulls last
