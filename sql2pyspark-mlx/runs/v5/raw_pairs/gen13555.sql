with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        carrier,
        delivered_date,
        row_number() over w_recent as row_seq,
        sum(cost) over w_recent_2 as status_sum_cost
    from shipments
    window w_recent as (partition by carrier order by cost desc, id), w_recent_2 as (partition by status, order_id)
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by carrier order by row_seq, id) as row_num
        from enriched
    ) t
    where row_num = 1
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        carrier,
        delivered_date,
        row_seq,
        status_sum_cost,
        row_num,
        date_trunc('WEEK', delivered_date) as delivered_date_period
    from deduped
    where carrier is not null
      and delivered_date >= to_date('2023-08-17')
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when delivered_date_period - lag(delivered_date_period) over (partition by carrier order by delivered_date_period, id) > interval 14 days then 1 else 0 end as new_group_flag
    from cleaned
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        carrier,
        delivered_date,
        row_num,
        delivered_date_period,
        sum(new_group_flag) over (partition by carrier order by delivered_date_period, id rows between unbounded preceding and current row) as session_no
    from gaps
),
cleaned_shipments as (
    -- Keep recent, valid rows
    select
        id,
        delivered_date,
        row_num,
        delivered_date_period,
        session_no,
        last_day(delivered_date) as delivered_date_month_end
    from sessionized
    where carrier is not null
),
enriched_shipments as (
    -- Window metrics over several different partitions
    select
        id,
        delivered_date,
        sum(session_no) over w_grp as running_sum_session_no,
        avg(session_no) over w_grp as running_avg_session_no,
        count(session_no) over w_grp as running_count_session_no,
        max(session_no) over w_grp as running_max_session_no,
        rank() over w_hist as session_no_rank
    from cleaned_shipments
    window w_grp as (order by delivered_date_period, id rows between unbounded preceding and current row), w_hist as (order by session_no desc)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(running_sum_session_no) over (order by running_sum_session_no, id) as prev_running_sum_session_no,
        lead(running_sum_session_no) over (order by running_sum_session_no, id) as next_running_sum_session_no,
        lead(running_avg_session_no) over (order by running_sum_session_no, id) as next_running_avg_session_no,
        row_number() over (order by running_sum_session_no, id) as row_seq
    from enriched_shipments
    where running_sum_session_no between 14 and 17
)
-- Grouped metrics
select
    running_count_session_no,
    date_trunc('quarter', delivered_date) as quarter_start,
    count(*) as num_rows,
    approx_percentile(running_count_session_no, 0.5) as running_count_session_no_p50,
    count(*) as row_count,
    count(distinct id) as distinct_ids,
    percentile_approx(next_running_avg_session_no, array(0.25, 0.5, 0.75)) as next_running_avg_session_no_quartiles,
    percent_rank() over (partition by running_count_session_no order by count(*) desc) as num_rows_percent_rank
from with_windows
group by running_count_session_no, quarter_start
order by row_count desc nulls first
