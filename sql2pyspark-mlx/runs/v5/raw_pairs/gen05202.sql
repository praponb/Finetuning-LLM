with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        starts_on,
        ends_on,
        date_trunc('MONTH', ends_on) as ends_on_period
    from promotions
    where channel is not null
      and starts_on > date '2023-04-15'
),
with_prev as (
    -- Previous event time per key
    select *, lag(ends_on_period) over (partition by channel order by ends_on_period, id) as prev_ends_on_period
    from filtered
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_ends_on_period is null or ends_on_period > prev_ends_on_period + interval '60' day then 1 else 0 end as is_new_burst
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        channel,
        starts_on,
        ends_on,
        ends_on_period,
        sum(is_new_burst) over (partition by channel order by ends_on_period, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when ends_on_period - lag(ends_on_period) over (partition by channel order by ends_on_period, id) > interval '60' day then 1 else 0 end as is_new_session
    from islands
),
sessions_promotions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by channel order by ends_on_period, id rows between unbounded preceding and current row) as burst_id_2
    from flagged_2
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(ends_on_period) over (partition by channel order by ends_on_period, id) is null
                 or ends_on_period > lag(ends_on_period) over (partition by channel order by ends_on_period, id) + interval '30' day then 1 else 0 end as is_new_burst
    from sessions_promotions
),
islands_promotions as (
    -- Running sum of the flag = session number
    select
        id,
        channel,
        ends_on_period,
        burst_id_2,
        sum(is_new_burst) over (partition by channel order by ends_on_period, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    id,
    channel,
    ends_on_period,
    burst_id_2
from islands_promotions
order by channel desc nulls last, id;
