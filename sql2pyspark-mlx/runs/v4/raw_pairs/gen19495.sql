with metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        last_value(browser) ignore nulls over (order by started_at asc, id asc rows between unbounded preceding and current row) as last_known_browser,
        first_value(country, true) over (partition by country, customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as first_known_country,
        first_value(browser) ignore nulls over w_part as first_known_browser
    from sessions
    window w_part as (partition by browser order by started_at asc, id asc rows between unbounded preceding and current row)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by browser order by started_at desc nulls last, id asc) as row_num
    from metrics
    qualify row_num = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at asc, id asc) is null
                 or started_at > lag(started_at) over (partition by country order by started_at asc, id asc) + interval '90' day then 1 else 0 end as new_group_flag
    from latest
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by country order by started_at asc, id asc) as prev_started_at
    from islands
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 90 days then 1 else 0 end as is_new_session
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
)
-- Ranking and gap features
select
    *,
    avg(island_id) over (partition by device order by cast(started_at as bigint) asc range between 7776000 preceding and current row) as island_id_avg_90d,
    sum(island_id) over (partition by device order by started_at asc, id asc rows between unbounded preceding and current row) as cumulative_island_id
from sessionized;
