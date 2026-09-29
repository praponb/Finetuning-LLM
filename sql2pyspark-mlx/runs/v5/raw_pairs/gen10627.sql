with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        to_date(txn_time) as txn_time_date,
        coalesce(merchant, 'NONE') as resolved_merchant
    from transactions
    where txn_time > date '2023-08-21'
      and is_flagged = false
),
with_dims as (
    -- Attach accounts attributes
    select
        s.*,
        a.account_type,
        a.opened_date,
        a.status
    from filtered s
    left join accounts a
        on s.account_id = a.id
    where a.account_type != 'CHECKING'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by txn_type, status order by amount nulls first) as rn
        from with_dims
    ) t
    where rn = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by status order by txn_time, id) > interval '14' day then 1 else 0 end as session_start_flag
    from ranked
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        txn_time_date,
        account_type,
        opened_date,
        status,
        rn,
        sum(session_start_flag) over (partition by status order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(amount) over (partition by status order by txn_time_date, id rows between 6 preceding and current row) as last7_avg_amount,
        count(amount) over (partition by status order by txn_time_date, id rows between 6 preceding and current row) as last7_count_amount,
        max(amount) over (partition by status order by txn_time_date, id rows between 6 preceding and current row) as last7_max_amount,
        min(amount) over (partition by status order by txn_time_date, id rows between 6 preceding and current row) as last7_min_amount
    from islands
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lag(last7_min_amount) over (partition by status order by last7_count_amount, id) as prev_last7_min_amount,
        lead(last7_min_amount) over (partition by status order by last7_count_amount, id) as next_last7_min_amount,
        lead(last7_avg_amount) over (partition by status order by last7_count_amount, id) as next_last7_avg_amount,
        row_number() over (partition by status order by last7_count_amount, id) as event_seq
    from metrics
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by status order by txn_time, id) > interval 14 days then 1 else 0 end as is_new_burst
    from scored
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by status order by txn_time, id rows between unbounded preceding and current row) as session_no
    from gaps
)
-- Join a dimension and aggregate
select
    d.status as account_status,
    src.status as transaction_status,
    count(*) as rows_total,
    min(src.event_seq) as min_event_seq,
    sum(src.next_last7_min_amount) as total_next_last7_min_amount
from sessions_transactions src
inner join accounts d on src.account_id = d.id
group by cube (d.status, src.status);
