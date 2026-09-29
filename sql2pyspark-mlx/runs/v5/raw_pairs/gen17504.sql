with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        date_trunc('year', starts_on) as starts_on_period,
        upper(channel) as channel_upper,
        coalesce(channel, 'N/A') as resolved_channel
    from promotions
    where discount_pct between 0.15 and 0.26
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by promo_code order by ends_on desc, id) as row_num
    from cleaned
    qualify row_num = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when starts_on_period - lag(starts_on_period) over (partition by promo_code order by starts_on_period, id) > interval 60 days then 1 else 0 end as session_start_flag
    from ranked
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by promo_code order by starts_on_period, id rows between unbounded preceding and current row) as session_no
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        promo_code,
        ends_on,
        discount_pct,
        channel_upper,
        resolved_channel,
        session_no,
        avg(session_start_flag) over w_running as running_avg_session_start_flag,
        max(session_start_flag) over w_running as running_max_session_start_flag,
        sum(session_start_flag) over w_running as running_sum_session_start_flag
    from sessionized
    window w_running as (partition by promo_code order by session_no, id rows between unbounded preceding and current row)
)
-- Aggregate with conditional metrics
select
    date_trunc('week', ends_on) as week_start,
    round(avg(discount_pct), 2) as avg_discount_pct,
    percentile_approx(running_avg_session_start_flag, array(0.25, 0.5, 0.75)) as running_avg_session_start_flag_quartiles,
    count(distinct promo_code) as distinct_promo_codes
from enriched
group by week_start;
