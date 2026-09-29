with lines as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) e as key, value
),
scored as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by key order by status_code) status_code_percent_rank,
        round(status_code / sum(status_code) over (partition by key), 4) status_code_share,
        max(status_code) over (partition by key order by cast(log_ts as bigint) range between 7200 preceding and current row) status_code_max_2h,
        rank() over (partition by key order by status_code desc) status_code_rank,
        cume_dist() over (order by status_code desc) status_code_cume_dist
    from lines
),
scored_app_logs as (
    -- Ranking and gap features
    select
        *,
        avg(status_code_percent_rank) over (partition by key order by cast(log_ts as bigint) range between 1800 preceding and current row) status_code_percent_rank_avg_30m,
        percent_rank() over (partition by key order by status_code_percent_rank nulls first) status_code_percent_rank_percent_rank,
        percent_rank() over (order by status_code_cume_dist nulls last) status_code_cume_dist_percent_rank
    from scored
)
-- Running and moving aggregates
select
    *,
    percent_rank() over w_running status_code_cume_dist_percent_rank_2,
    round(status_code_rank / sum(status_code_rank) over (partition by key), 4) status_code_rank_share,
    max(status_code_cume_dist_percent_rank) over w_recent status_code_cume_dist_percent_rank_max_2h,
    sum(status_code_share) over w_recent status_code_share_sum_2h
from scored_app_logs
window w_running as (partition by key order by status_code_cume_dist desc nulls last), w_recent as (order by cast(log_ts as bigint) range between 7200 preceding and current row);
