with filtered as (
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        length(status) as status_length,
        date_format(paid_at, 'MMM yyyy') as paid_at_label
    from payments
    where paid_at > timestamp '2023-07-01 09:00:00'
      and order_id in (19, 30, 31, 33)
),
with_dims as (
    select
        a.id,
        a.order_id,
        a.method,
        a.paid_at,
        a.status_length,
        a.paid_at_label,
        o.channel,
        o.status as order_status
    from filtered a
    join orders o
        on a.order_id = o.id
),
metrics as (
    select
        id,
        order_id,
        method,
        status_length,
        paid_at_label,
        channel,
        order_status,
        sum(status_length) over (partition by method order by status_length, id rows between unbounded preceding and current row) as cumulative_status_length,
        stddev_samp(status_length) over (partition by channel, order_id) as channel_stddev_samp_status_length,
        row_number() over (partition by order_status order by status_length desc, id) as seq_no
    from with_dims
),
deduped as (
    select
        id,
        order_id,
        method,
        paid_at_label,
        order_status,
        cumulative_status_length,
        seq_no
    from (
        select *, rank() over (partition by method order by status_length) as rn
        from metrics
    ) t
    where rn = 1
),
joined as (
    select src.*, b.shipping_fee
    from deduped src
    join orders b
        on src.order_id = b.id
),
with_windows as (
    select
        *,
        lag(seq_no, 1) over (partition by order_id, order_status order by shipping_fee, id) as prev_seq_no,
        lag(shipping_fee, 2) over (partition by method order by shipping_fee, id) as prev_shipping_fee,
        last_value(shipping_fee) ignore nulls over (partition by method order by cumulative_status_length, id rows between unbounded preceding and current row) as last_known_shipping_fee,
        lag(seq_no, 1) over (partition by method order by cumulative_status_length, id) as prev_seq_no_2,
        first_value(order_status) ignore nulls over (order by cumulative_status_length, id rows between unbounded preceding and current row) as first_known_order_status
    from joined
    where order_status in ('COMPLETED', 'CANCELLED', 'RETURNED')
)
select
    case when prev_seq_no_2 >= 9 then 'HIGH' else 'LOW' end as prev_seq_no_2_tier,
    method,
    avg(seq_no) filter (where method = 'bank_transfer') as bank_transfer_avg_seq_no,
    round(avg(prev_seq_no_2), 2) as avg_prev_seq_no_2,
    percentile_approx(shipping_fee, 0.5) as shipping_fee_p50,
    count(case when order_status = 'COMPLETED' then 1 end) as completed_count,
    count(*) as row_count
from with_windows
group by prev_seq_no_2_tier, method
having bank_transfer_avg_seq_no < 1000000;
