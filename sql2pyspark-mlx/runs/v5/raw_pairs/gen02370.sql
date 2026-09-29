with cleaned as (
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        substring(merchant, 1, 2) as merchant_prefix,
        cast(is_flagged as int) as is_flagged_flag,
        coalesce(merchant, 'NONE') as merchant_clean
    from transactions
    where txn_time >= '2023-03-01'
),
with_gaps as (
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) > interval '7' day then 1 else 0 end as session_start_flag
    from cleaned
),
islands as (
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        merchant_prefix,
        is_flagged_flag,
        merchant_clean,
        sum(session_start_flag) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
),
deduped as (
    select *, row_number() over (partition by txn_type order by is_flagged_flag asc nulls last, id asc) as latest_rank
    from islands
    qualify latest_rank = 1
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by txn_type order by is_flagged_flag desc nulls first, id asc) as dedup_rank
        from deduped
    ) t
    where dedup_rank = 1
),
with_gaps_2 as (
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) + interval '7' day then 1 else 0 end as is_new_session
    from ranked
),
sessions_transactions as (
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        merchant_prefix,
        is_flagged_flag,
        merchant_clean,
        session_id,
        sum(is_new_session) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as island_id
    from with_gaps_2
)
select
    txn_type,
    count(case when txn_type = 'FEE' then 1 end) as fee_count,
    sort_array(collect_set(txn_type)) as txn_type_list,
    avg(is_flagged_flag) as avg_is_flagged_flag,
    round(sum(island_id) / nullif(count(case when txn_type = 'REFUND' then 1 end), 0), 2) as island_id_per_refund,
    count(*) filter (where txn_type = 'FEE') as fee_rows
from sessions_transactions
group by txn_type
order by fee_count desc nulls last
