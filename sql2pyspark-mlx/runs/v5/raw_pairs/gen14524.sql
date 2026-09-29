with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        lower(status) as status_lower
    from orders
    where channel is distinct from 'mobile'
      and discount < 0.22
),
enriched as (
    -- Ranking and gap features
    select
        *,
        count(amount) over w_recent as running_count_amount,
        sum(amount) over w_recent as running_sum_amount,
        avg(amount) over w_recent as running_avg_amount
    from cleaned
    window w_recent as (partition by store_id order by discount, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select *, round(discount, 2) as discount_rounded
    from enriched
    where channel is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by status order by running_sum_amount desc nulls last, id) as latest_rank
    from base
    qualify latest_rank = 1
)
-- Multi-level aggregation
select
    status,
    count(case when status = 'COMPLETED' then 1 end) as completed_count,
    sum(case when status = 'CANCELLED' then running_count_amount else 0 end) as cancelled_running_count_amount,
    count(*) as event_count,
    avg(running_avg_amount) filter (where status = 'RETURNED') as returned_avg_running_avg_amount,
    count(*) as event_count_2
from deduped
group by status
order by cancelled_running_count_amount;
