with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        max(amount) over (partition by method, order_id order by amount, id rows between 6 preceding and current row) as last7_max_amount,
        min(amount) over (partition by method, order_id order by amount, id rows between 6 preceding and current row) as last7_min_amount,
        sum(amount) over (partition by method, order_id order by amount, id rows between 6 preceding and current row) as last7_sum_amount
    from payments
),
enriched_payments as (
    -- Attach orders attributes
    select
        a.*,
        b.discount,
        b.order_date
    from enriched a
    left join orders b
        on a.order_id <=> b.id
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        amount,
        status,
        last7_max_amount,
        last7_sum_amount,
        order_date,
        lead(last7_sum_amount) over w_ordered as next_last7_sum_amount,
        lag(last7_sum_amount) over w_ordered as prev_last7_sum_amount,
        lead(amount) over w_ordered as next_amount,
        row_number() over w_ordered as seq_no
    from enriched_payments
    where order_date <= '2024-10-01'
    window w_ordered as (partition by status, method order by paid_at, id)
),
with_dims as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.last7_max_amount,
        e.last7_sum_amount,
        e.order_date,
        e.next_last7_sum_amount,
        e.next_amount,
        e.seq_no,
        b.shipping_fee,
        b.channel,
        b.discount
    from scored e
    left join orders b
        on e.order_id = b.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by order_id, channel order by seq_no desc, id) as row_num
    from with_dims
    qualify row_num = 1
)
-- Aggregate with conditional metrics
select
    date_trunc('month', order_date) as activity_month,
    count(distinct id) as distinct_ids,
    approx_percentile(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
    sum(case when channel = 'mobile' then last7_sum_amount else 0 end) as mobile_last7_sum_amount
from ranked
group by activity_month
order by activity_month desc;
