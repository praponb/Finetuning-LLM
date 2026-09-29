with flagged as (
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time, id) > interval '30' day then 1 else 0 end as session_start_flag
    from transactions
),
sessionized as (
    select
        id,
        txn_time,
        merchant,
        is_flagged,
        sum(session_start_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    case when burst_id >= 8 then 'PREMIUM' else 'STANDARD' end as burst_id_tier,
    count(*) filter (where merchant = '7-Eleven') as 7_eleven_rows,
    count(case when merchant = '7-Eleven' then 1 end) as 7_eleven_count,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as duration_min
from sessionized
group by 1;
