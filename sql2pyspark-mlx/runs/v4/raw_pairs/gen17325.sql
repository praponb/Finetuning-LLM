with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        start_date,
        end_date,
        status,
        substring(status, 1, 3) as status_prefix
    from subscriptions
    where monthly_fee >= 220
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by plan order by start_date, id) as event_seq,
        row_number() over w_ordered as row_seq,
        row_number() over w_ordered as row_seq_2
    from filtered
    window w_ordered as (partition by status order by start_date desc, id)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        start_date,
        end_date,
        status,
        status_prefix,
        event_seq,
        row_seq,
        row_seq_2,
        ntile(3) over (partition by plan order by row_seq, id) as row_seq_bucket,
        cume_dist() over (partition by status order by row_seq_2 desc) as row_seq_2_cume_dist,
        cume_dist() over (order by event_seq nulls last) as event_seq_cume_dist
    from scored
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by plan, status order by row_seq_2_cume_dist nulls first, id) as rn
    from enriched
    qualify rn = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(3) over (partition by plan order by row_seq desc, id) as row_seq_bucket_2,
        round(row_seq / sum(row_seq) over (partition by status), 4) as row_seq_share,
        lead(event_seq, 1) over (partition by plan order by row_seq_2_cume_dist, id) as next_event_seq
    from latest
),
with_windows_subscriptions as (
    -- Ranking and gap features
    select
        id,
        plan,
        start_date,
        end_date,
        status,
        status_prefix,
        event_seq,
        row_seq,
        row_seq_2,
        row_seq_bucket,
        row_seq_2_cume_dist,
        event_seq_cume_dist,
        rn,
        row_seq_bucket_2,
        row_seq_share,
        next_event_seq,
        lag(row_seq_2, 2) over (partition by status order by row_seq_share, id) as prev_row_seq_2,
        lead(next_event_seq, 2, 0) over (order by end_date, id) as next_next_event_seq,
        sum(row_seq_2_cume_dist) over (partition by plan order by end_date, id rows between unbounded preceding and current row) as cumulative_row_seq_2_cume_dist
    from with_windows
),
latest_subscriptions as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by plan order by row_seq_bucket_2 desc) as rn_2
    from with_windows_subscriptions
    qualify rn_2 = 1
)
-- Running and moving aggregates
select
    *,
    ntile(5) over (order by row_seq desc, id) as row_seq_bucket_3,
    round(event_seq / sum(event_seq) over (partition by status), 4) as event_seq_share,
    sum(event_seq) over (partition by status order by rn_2, id rows between unbounded preceding and current row) as running_event_seq,
    ntile(3) over w_grp as row_seq_bucket_2_bucket
from latest_subscriptions
window w_grp as (partition by plan order by row_seq_bucket_2 desc, id)
