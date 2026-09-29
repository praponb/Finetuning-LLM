with scored as (
    -- Running and moving aggregates
    select
        *,
        lead(amount) over w_recent as next_amount,
        lag(amount) over w_recent as prev_amount,
        row_number() over w_recent as row_seq,
        row_number() over (partition by order_id, method order by amount desc, id) as row_seq_2
    from payments
    where status in ('REFUNDED', 'SUCCESS')
    window w_recent as (partition by order_id order by paid_at, id)
)
-- Multi-level aggregation
select
    status,
    order_id,
    size(collect_set(method)) as num_methods,
    avg(next_amount) filter (where status = 'SUCCESS') as success_avg_next_amount,
    count_if(status = 'FAILED') as failed_events,
    count(distinct id) as distinct_ids,
    grouping_id() as grouping_level
from scored
group by rollup (status, order_id)
order by distinct_ids desc nulls last
