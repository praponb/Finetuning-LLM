with gaps as (
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval '7' day then 1 else 0 end as is_new_session
    from payments
),
islands as (
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        sum(is_new_session) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
select
    case when session_id >= 6 then 'HIGH' else 'LOW' end as session_id_tier,
    count(distinct order_id) as distinct_order_ids,
    stddev_samp(amount) as amount_stddev,
    round(avg(session_id), 2) as avg_session_id,
    count(case when status = 'SUCCESS' then 1 end) as success_count
from islands
group by 1
order by amount_stddev;
