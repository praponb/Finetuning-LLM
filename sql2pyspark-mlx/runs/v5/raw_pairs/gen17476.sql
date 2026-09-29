with cleaned as (
    select
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        datediff(end_date, start_date) days_start_date_to_end_date
    from subscriptions
    where end_date > date '2023-04-12'
      and status = 'EXPIRED'
),
deduped as (
    select *
    from cleaned
    qualify row_number() over (partition by status order by monthly_fee desc, id asc) = 1
),
deduped_subscriptions as (
    select
        id,
        end_date,
        days_start_date_to_end_date
    from (
        select *, rank() over (partition by status order by start_date asc nulls last) as rank_in_group
        from deduped
    ) ranked
    where rank_in_group = 1
),
metrics as (
    select
        *,
        avg(days_start_date_to_end_date) over (order by days_start_date_to_end_date asc, id asc rows between 2 preceding and current row) days_start_date_to_end_date_avg_2,
        sum(days_start_date_to_end_date) over (order by days_start_date_to_end_date asc, id asc rows between unbounded preceding and current row) cumulative_days_start_date_to_end_date,
        rank() over (order by days_start_date_to_end_date desc nulls first) days_start_date_to_end_date_rank,
        cume_dist() over (order by days_start_date_to_end_date desc nulls first) days_start_date_to_end_date_cume_dist
    from deduped_subscriptions
),
with_windows as (
    select
        id,
        end_date,
        days_start_date_to_end_date_avg_2,
        days_start_date_to_end_date_rank,
        sum(days_start_date_to_end_date) over (order by days_start_date_to_end_date_avg_2 asc, id asc rows between unbounded preceding and current row) running_days_start_date_to_end_date,
        first_value(days_start_date_to_end_date, true) over (order by days_start_date_to_end_date_rank asc, id asc rows between unbounded preceding and current row) first_known_days_start_date_to_end_date,
        avg(days_start_date_to_end_date) over (order by end_date asc, id asc rows between 2 preceding and current row) days_start_date_to_end_date_avg_2_2,
        avg(cumulative_days_start_date_to_end_date) over (order by days_start_date_to_end_date_avg_2 asc, id asc rows between 3 preceding and 1 preceding) cumulative_days_start_date_to_end_date_avg_3_prior,
        avg(days_start_date_to_end_date) over (order by cumulative_days_start_date_to_end_date asc, id asc rows between 5 preceding and 1 preceding) days_start_date_to_end_date_avg_5_prior
    from metrics
    where days_start_date_to_end_date = 58
),
with_windows_subscriptions as (
    select
        id,
        end_date,
        days_start_date_to_end_date_avg_2,
        running_days_start_date_to_end_date,
        first_known_days_start_date_to_end_date,
        cumulative_days_start_date_to_end_date_avg_3_prior,
        days_start_date_to_end_date_avg_5_prior,
        avg(days_start_date_to_end_date_avg_2) over (order by days_start_date_to_end_date_rank asc, id asc rows between unbounded preceding and current row) running_avg_days_start_date_to_end_date_avg_2,
        min(days_start_date_to_end_date_avg_2) over (order by days_start_date_to_end_date_rank asc, id asc rows between unbounded preceding and current row) running_min_days_start_date_to_end_date_avg_2,
        count(days_start_date_to_end_date_avg_2) over (order by days_start_date_to_end_date_rank asc, id asc rows between unbounded preceding and current row) running_count_days_start_date_to_end_date_avg_2
    from with_windows
),
scored as (
    select
        *,
        avg(running_avg_days_start_date_to_end_date_avg_2) over w_running running_avg_days_start_date_to_end_date_avg_2_avg_6_prior,
        first_value(running_avg_days_start_date_to_end_date_avg_2) ignore nulls over w_grp first_known_running_avg_days_start_date_to_end_date_avg_2,
        ntile(5) over w_ordered running_min_days_start_date_to_end_date_avg_2_bucket,
        last_value(running_days_start_date_to_end_date) ignore nulls over w_ordered_2 last_known_running_days_start_date_to_end_date
    from with_windows_subscriptions
    window w_running as (order by days_start_date_to_end_date_avg_2 asc, id asc rows between 6 preceding and 1 preceding), w_grp as (order by days_start_date_to_end_date_avg_2 asc, id asc rows between unbounded preceding and current row), w_ordered as (order by running_min_days_start_date_to_end_date_avg_2 asc, id asc), w_ordered_2 as (order by first_known_days_start_date_to_end_date asc, id asc rows between unbounded preceding and current row)
),
daily as (
    select
        date_trunc('week', end_date) as week_start,
        case when cumulative_days_start_date_to_end_date_avg_3_prior >= 400 then 'LARGE' else 'SMALL' end as cumulative_days_start_date_to_end_date_avg_3_prior_tier,
        round(avg(first_known_running_avg_days_start_date_to_end_date_avg_2), 2) as avg_first_known_running_avg_days_start_date_to_end_date_avg_2,
        percentile_approx(running_avg_days_start_date_to_end_date_avg_2, 0.5) as running_avg_days_start_date_to_end_date_avg_2_p50
    from scored
    group by date_trunc('week', end_date), case when cumulative_days_start_date_to_end_date_avg_3_prior >= 400 then 'LARGE' else 'SMALL' end
)
select
    week_start,
    cumulative_days_start_date_to_end_date_avg_3_prior_tier,
    avg_first_known_running_avg_days_start_date_to_end_date_avg_2,
    running_avg_days_start_date_to_end_date_avg_2_p50
from daily
order by running_avg_days_start_date_to_end_date_avg_2_p50 asc;
