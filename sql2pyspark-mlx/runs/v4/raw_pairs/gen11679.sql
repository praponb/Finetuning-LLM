with filtered as (
    -- Basic cleansing and defaults
    select *, case when is_flagged then 'Yes' else 'No' end as is_flagged_label
    from transactions
    where merchant is not null
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by account_id order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by account_id order by txn_time asc, id asc) + interval '7' day then 1 else 0 end as is_new_burst
    from filtered
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by account_id order by txn_time asc, id asc rows between unbounded preceding and current row) as session_no
    from flagged
)
-- Aggregate with conditional metrics
select
    session_no,
    merchant,
    count(*) as num_rows,
    avg(is_new_burst) filter (where merchant = 'Lazada') as lazada_avg_is_new_burst,
    percentile_approx(session_no, array(0.25, 0.5, 0.75)) as session_no_quartiles,
    grouping_id() as grouping_level
from sessions_transactions
group by grouping sets ((session_no, merchant), (session_no), ())
order by merchant desc
