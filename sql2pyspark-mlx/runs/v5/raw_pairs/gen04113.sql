with filtered as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        datediff(current_date(), starts_on) days_since_starts_on,
        trim(promo_code) clean_promo_code,
        coalesce(channel, 'DIRECT') channel_clean
    from promotions
    where channel is not distinct from 'store'
      and channel is not null
),
latest as (
    select *, row_number() over (partition by promo_code order by starts_on desc, id asc) as latest_rank
    from filtered
    qualify latest_rank = 1
),
scored as (
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        days_since_starts_on,
        channel_clean,
        latest_rank,
        count(days_since_starts_on) over w_ordered running_count_days_since_starts_on,
        avg(days_since_starts_on) over w_ordered running_avg_days_since_starts_on,
        max(days_since_starts_on) over w_ordered running_max_days_since_starts_on
    from latest
    where promo_code = 'VIP'
    window w_ordered as (partition by promo_code order by ends_on asc, id asc rows between unbounded preceding and current row)
),
scored_promotions as (
    select
        id,
        starts_on,
        ends_on,
        days_since_starts_on,
        channel_clean,
        latest_rank,
        running_avg_days_since_starts_on,
        running_max_days_since_starts_on,
        avg(days_since_starts_on) over (partition by promo_code order by running_max_days_since_starts_on asc, id asc rows between 5 preceding and current row) days_since_starts_on_avg_5,
        avg(running_count_days_since_starts_on) over (partition by promo_code order by starts_on asc, id asc rows between 2 preceding and current row) running_count_days_since_starts_on_avg_2,
        sum(days_since_starts_on) over w_hist cumulative_days_since_starts_on
    from scored
    window w_hist as (partition by promo_code order by running_count_days_since_starts_on asc, id asc rows between unbounded preceding and current row)
),
scored_promotions_2 as (
    select
        id,
        starts_on,
        latest_rank,
        running_max_days_since_starts_on,
        lag(running_avg_days_since_starts_on) over (order by cumulative_days_since_starts_on asc, id asc) prev_running_avg_days_since_starts_on,
        lead(running_avg_days_since_starts_on) over (order by cumulative_days_since_starts_on asc, id asc) next_running_avg_days_since_starts_on,
        lag(running_count_days_since_starts_on_avg_2) over (order by cumulative_days_since_starts_on asc, id asc) prev_running_count_days_since_starts_on_avg_2,
        lead(running_count_days_since_starts_on_avg_2) over (order by cumulative_days_since_starts_on asc, id asc) next_running_count_days_since_starts_on_avg_2,
        row_number() over (order by cumulative_days_since_starts_on asc, id asc) row_seq
    from scored_promotions
)
select
    date_trunc('week', starts_on) as week_start,
    min(prev_running_count_days_since_starts_on_avg_2) as min_prev_running_count_days_since_starts_on_avg_2,
    count(distinct id) as distinct_ids
from scored_promotions_2
group by week_start
order by min_prev_running_count_days_since_starts_on_avg_2 asc
