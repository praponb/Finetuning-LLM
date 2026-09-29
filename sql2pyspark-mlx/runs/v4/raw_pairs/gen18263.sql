with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) > interval '90' day then 1 else 0 end as session_start_flag
    from transactions
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as burst_id
    from gaps
)
-- Grouped metrics
select
    case when burst_id >= 7 then 'HIGH' else 'LOW' end as burst_id_tier,
    date_trunc('quarter', txn_time) as quarter_start,
    count(*) filter (where merchant = 'Starbucks') as starbucks_rows,
    count(*) filter (where merchant = 'Starbucks') as starbucks_rows_2,
    round(sum(session_start_flag) / nullif(count(case when txn_type = 'FEE' then 1 end), 0), 2) as session_start_flag_per_fee,
    count(*) as num_rows,
    min(txn_time) as first_txn_time
from sessionized
group by grouping sets ((case when burst_id >= 7 then 'HIGH' else 'LOW' end, date_trunc('quarter', txn_time)), (case when burst_id >= 7 then 'HIGH' else 'LOW' end), ())
