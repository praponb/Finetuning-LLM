with cleaned as (
    select
        id,
        order_id,
        method,
        amount,
        status,
        length(method) method_length,
        round(amount, 0) amount_rounded,
        coalesce(method, 'OTHER') method_clean
    from payments
    where paid_at > date '2023-08-17'
      and order_id is not null
),
with_windows as (
    select
        *,
        sum(amount) over (order by amount, id rows between unbounded preceding and current row) cumulative_amount,
        lead(amount_rounded, 1) over w_part next_amount_rounded,
        last_value(amount_rounded, true) over (partition by method order by amount_rounded, id rows between unbounded preceding and current row) last_known_amount_rounded,
        row_number() over (order by method_length, id) row_seq,
        lead(amount, 1) over (partition by order_id, method order by amount_rounded, id) next_amount
    from cleaned
    window w_part as (partition by method, order_id order by method_length, id)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by status order by next_amount nulls last, id) as row_num
        from with_windows
    ) x
    where row_num = 1
),
with_windows_payments as (
    select
        id,
        order_id,
        method,
        amount,
        status,
        method_length,
        amount_rounded,
        method_clean,
        cumulative_amount,
        next_amount_rounded,
        last_known_amount_rounded,
        row_seq,
        next_amount,
        row_num,
        first_value(next_amount_rounded) ignore nulls over w_part first_known_next_amount_rounded,
        dense_rank() over (partition by order_id order by method_length desc) method_length_dense_rank,
        row_number() over (partition by status order by method_length, id) event_seq,
        min(amount) over w_recent status_min_amount,
        row_number() over (partition by method order by method_length, id) seq_no
    from ranked
    window w_part as (partition by order_id order by row_num, id rows between unbounded preceding and current row), w_recent as (partition by status)
),
joined as (
    select
        e.*,
        p.amount as order_amount,
        p.shipping_fee,
        p.order_date
    from with_windows_payments e
    inner join orders p
        on e.order_id = p.id
)
select
    s.id,
    s.order_id,
    s.method,
    s.method_length,
    s.amount_rounded,
    s.method_clean,
    s.cumulative_amount,
    s.next_amount_rounded,
    s.row_num,
    (select sum(o2.unit_price) from order_items o2 where o2.order_id = s.order_id) as sum_order_item_unit_price
from joined s
order by method nulls first, method_length
