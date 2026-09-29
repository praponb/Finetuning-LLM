with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        method,
        amount,
        paid_at,
        status,
        substring(method, 1, 2) as method_prefix
    from payments
    where status in ('FAILED', 'REFUNDED', 'SUCCESS')
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status order by amount desc nulls first, id) as row_num
    from filtered
    qualify row_num = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 60 days then 1 else 0 end as is_new_burst
    from ranked
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
valid as (
    -- Keep recent, valid rows
    select *, ceil(is_new_burst) as is_new_burst_whole
    from sessionized
    where paid_at > to_timestamp('2023-11-20 11:00:00')
      and is_new_burst >= 0
)
-- Grouped metrics
select
    case when burst_id >= 8 then 'HIGH' else 'LOW' end as burst_id_tier,
    case when is_new_burst_whole >= 62 then 'LARGE' else 'SMALL' end as is_new_burst_whole_tier,
    date_trunc('week', paid_at) as week_start,
    sort_array(collect_set(method)) as method_list,
    count(case when status = 'SUCCESS' then 1 end) as success_count,
    round(avg(is_new_burst_whole), 2) as avg_is_new_burst_whole,
    percent_rank() over (partition by case when burst_id >= 8 then 'HIGH' else 'LOW' end order by round(avg(is_new_burst_whole), 2) desc) as avg_is_new_burst_whole_percent_rank
from valid
group by case when burst_id >= 8 then 'HIGH' else 'LOW' end, case when is_new_burst_whole >= 62 then 'LARGE' else 'SMALL' end, date_trunc('week', paid_at)
order by burst_id_tier desc nulls first;
