with enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        row_number() over (partition by customer_id order by opened_date desc, id) as row_seq,
        row_number() over (partition by account_type, customer_id order by balance, id) as event_seq,
        min(balance) over (partition by customer_id) as customer_id_min_balance,
        lag(balance, 2) over (partition by account_type order by opened_date, id) as prev_balance,
        first_value(status, true) over (partition by status order by balance, id rows between unbounded preceding and current row) as first_known_status
    from accounts
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.opened_date,
        s.status,
        s.event_seq,
        top1.merchant,
        top1.transaction_txn_time
    from enriched s
    left join lateral (
            select c.merchant, c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by status order by opened_date nulls first, id) as rank_in_group
        from with_last
    ) x
    where rank_in_group = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(transaction_txn_time) over (partition by status order by transaction_txn_time, id) is null
                 or transaction_txn_time > lag(transaction_txn_time) over (partition by status order by transaction_txn_time, id) + interval 14 days then 1 else 0 end as is_new_burst
    from ranked
),
sessions_accounts as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by status order by transaction_txn_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
summary as (
    -- Grouped metrics
    select
        merchant,
        status,
        approx_percentile(event_seq, 0.5) as event_seq_p50,
        sum(session_no) filter (where status = 'CLOSED') as closed_total_session_no,
        count(case when merchant = 'Grab' then 1 end) as grab_count,
        percentile_approx(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles
    from sessions_accounts
    group by merchant, status
    having approx_percentile(event_seq, 0.5) < 1000000
)
select
    merchant,
    status,
    closed_total_session_no,
    grab_count,
    event_seq_quartiles
from summary;
