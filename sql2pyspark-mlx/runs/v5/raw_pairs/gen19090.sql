with base as (
    -- Basic cleansing and defaults
    select
        *,
        date_format(txn_time, 'MMM yyyy') as txn_time_label,
        coalesce(merchant, 'UNKNOWN') as merchant_clean
    from transactions
    where merchant is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time asc, id asc) > interval 30 days then 1 else 0 end as is_new_burst
    from base
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by merchant order by txn_time asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
)
-- Multi-level aggregation
select
    case when amount >= 4200 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    case when island_id >= 6 then 'HIGH' else 'LOW' end as island_id_tier,
    percentile_approx(island_id, 0.5) as island_id_p50,
    percentile_approx(island_id, array(0.25, 0.5, 0.75)) as island_id_quartiles,
    count(case when txn_type = 'CREDIT' then 1 end) as credit_count,
    sum(case when merchant = 'Shell' then island_id else 0 end) as shell_island_id
from islands
group by 1, 2
order by amount_tier asc nulls last;
