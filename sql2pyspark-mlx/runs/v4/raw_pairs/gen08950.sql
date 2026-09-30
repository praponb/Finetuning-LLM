with gaps as (
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval '14' day then 1 else 0 end as new_group_flag
    from sessions
),
sessions_sessions as (
    select *, sum(new_group_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_no
    from gaps
)
select
    date_trunc('quarter', started_at) as quarter_start,
    date_trunc('month', started_at) as activity_month,
    case when session_no >= 7 then 'LARGE' else 'SMALL' end as session_no_tier,
    sum(case when browser = 'Chrome' then new_group_flag else 0 end) as chrome_new_group_flag,
    sum(case when device = 'desktop' then new_group_flag else 0 end) as desktop_new_group_flag,
    round(avg(session_no), 2) as avg_session_no,
    percentile_approx(new_group_flag, array(0.25, 0.5, 0.75)) as new_group_flag_quartiles,
    percentile_approx(new_group_flag, array(0.25, 0.5, 0.75)) as new_group_flag_quartiles_2,
    dense_rank() over (partition by date_trunc('quarter', started_at) order by sum(case when browser = 'Chrome' then new_group_flag else 0 end) desc) as chrome_new_group_flag_dense_rank
from sessions_sessions
group by date_trunc('quarter', started_at), date_trunc('month', started_at), case when session_no >= 7 then 'LARGE' else 'SMALL' end
order by avg_session_no nulls first
