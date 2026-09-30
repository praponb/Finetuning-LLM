with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by country order by started_at, id) > interval '14' day then 1 else 0 end as new_group_flag
    from sessions
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device,
        started_at,
        country,
        sum(new_group_flag) over (partition by country order by started_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lead(burst_id) over (partition by device order by burst_id, id) as next_burst_id,
        row_number() over (partition by device order by burst_id, id) as event_seq
    from islands
),
with_windows_sessions as (
    -- Ranking and gap features
    select
        *,
        sum(burst_id) over (order by burst_id, id rows between unbounded preceding and current row) as running_burst_id,
        sum(next_burst_id) over (partition by country) as country_sum_next_burst_id
    from with_windows
),
enriched as (
    -- Ranking and gap features
    select
        id,
        device,
        country,
        next_burst_id,
        event_seq,
        last_value(running_burst_id) ignore nulls over w_part as last_known_running_burst_id,
        avg(country_sum_next_burst_id) over (partition by device order by started_at, id rows between 6 preceding and 1 preceding) as country_sum_next_burst_id_avg_6_prior
    from with_windows_sessions
    window w_part as (partition by country order by event_seq, id rows between unbounded preceding and current row)
)
select
    id,
    device,
    country,
    next_burst_id,
    event_seq,
    last_known_running_burst_id,
    country_sum_next_burst_id_avg_6_prior
from enriched
order by event_seq nulls first, country_sum_next_burst_id_avg_6_prior;
