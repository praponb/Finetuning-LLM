with base as (
    select *, case when channel = 'mobile' then 1 else 0 end as is_mobile
    from orders
    where order_date >= date '2023-05-20'
      and customer_id is not null
),
with_last as (
    select
        s.id,
        s.customer_id,
        s.store_id,
        s.status,
        s.channel,
        s.amount,
        s.is_mobile,
        x.payment_amount,
        x.payment_paid_at
    from base s
    join lateral (
            select c.amount as payment_amount, c.paid_at as payment_paid_at
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
with_gaps as (
    select *, case when payment_paid_at - lag(payment_paid_at) over (partition by channel order by payment_paid_at, id) > interval '90' day then 1 else 0 end as is_new_burst
    from with_last
),
islands as (
    select
        id,
        customer_id,
        channel,
        amount,
        is_mobile,
        payment_paid_at,
        sum(is_new_burst) over (partition by channel order by payment_paid_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
metrics as (
    select
        *,
        lag(amount) over (partition by customer_id, channel order by session_no, id) as prev_amount,
        lead(session_no) over (partition by customer_id, channel order by session_no, id) as next_session_no,
        row_number() over (partition by customer_id, channel order by session_no, id) as event_seq
    from islands
),
remaining_keys as (
    select id, channel
    from metrics
    except
    select id, channel
    from orders
    where customer_id in (2, 22, 23, 39)
),
verified as (
    select s.*
    from metrics s
    inner join remaining_keys k on s.id = k.id and s.channel = k.channel
),
totals as (
    select
        date_trunc('quarter', payment_paid_at) as quarter_start,
        case when is_mobile >= 72 then 'PREMIUM' else 'STANDARD' end as is_mobile_tier,
        date_trunc('year', payment_paid_at) as year_start,
        approx_percentile(next_session_no, 0.95) as next_session_no_p95,
        approx_percentile(session_no, 0.95) as session_no_p95,
        count(*) filter (where channel = 'web') as web_rows,
        count(distinct channel) as distinct_channels,
        dense_rank() over (partition by date_trunc('quarter', payment_paid_at) order by approx_percentile(next_session_no, 0.95) desc) as next_session_no_p95_dense_rank
    from verified
    group by quarter_start, is_mobile_tier, year_start
)
select
    quarter_start,
    is_mobile_tier,
    year_start,
    web_rows,
    distinct_channels,
    next_session_no_p95_dense_rank
from totals;
