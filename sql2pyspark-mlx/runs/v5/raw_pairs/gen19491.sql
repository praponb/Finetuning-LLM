with cleaned as (
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        length(status) as status_length
    from payments
    where paid_at >= to_timestamp('2023-09-19 01:00:00')
      and order_id is not null
),
latest as (
    select *
    from (
        select *, rank() over (partition by order_id order by status_length desc nulls last) as latest_rank
        from cleaned
    ) ranked
    where latest_rank = 1
),
filtered as (
    select
        id,
        order_id,
        method,
        status,
        latest_rank,
        status_length - latest_rank as status_length_minus_latest_rank,
        coalesce(method, 'N/A') as resolved_method
    from latest
    where paid_at >= to_date('2023-09-13')
),
with_windows as (
    select
        id,
        order_id,
        status,
        latest_rank,
        status_length_minus_latest_rank,
        resolved_method,
        avg(status_length_minus_latest_rank) over (partition by status, method) as status_avg_status_length_minus_latest_rank,
        ntile(4) over (order by status_length_minus_latest_rank, id) as status_length_minus_latest_rank_bucket,
        avg(status_length_minus_latest_rank) over (partition by order_id order by status_length_minus_latest_rank, id rows between 2 preceding and 1 preceding) as status_length_minus_latest_rank_avg_2_prior,
        ntile(3) over (partition by order_id order by status_length_minus_latest_rank, id) as status_length_minus_latest_rank_bucket_2
    from filtered
),
base as (
    select
        *,
        ceil(status_avg_status_length_minus_latest_rank) as status_avg_status_length_minus_latest_rank_whole,
        latest_rank + status_length_minus_latest_rank_bucket as latest_rank_plus_status_length_minus_latest_rank_bucket
    from with_windows
    where order_id is not null
),
enriched as (
    select
        s.id,
        s.order_id,
        s.status_length_minus_latest_rank,
        s.resolved_method,
        s.status_avg_status_length_minus_latest_rank,
        s.status_length_minus_latest_rank_avg_2_prior,
        s.status_length_minus_latest_rank_bucket_2,
        s.status_avg_status_length_minus_latest_rank_whole,
        p.channel,
        p.amount,
        p.order_date
    from base s
    inner join orders p
        on s.order_id = p.id
)
select
    d.channel as order_channel,
    s.channel as payment_channel,
    count(*) as rows_total,
    avg(s.amount) as avg_amount,
    min(s.status_length_minus_latest_rank) as min_status_length_minus_latest_rank,
    grouping_id() as gid
from enriched s
left join orders d on s.order_id = d.id
group by rollup (d.channel, s.channel)
