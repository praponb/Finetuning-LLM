with flagged as (
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '30' day then 1 else 0 end as is_new_burst
    from transactions
),
sessions_transactions as (
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        sum(is_new_burst) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
enriched as (
    select
        s.id,
        s.account_id,
        s.txn_type,
        s.merchant,
        s.session_no,
        a.account_type,
        a.opened_date,
        a.status
    from sessions_transactions s
    left join accounts a
        on s.account_id = a.id
    where month(a.opened_date) in (1, 3, 12)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by account_id, merchant order by session_no desc, id) as latest_rank
        from enriched
    ) t
    where latest_rank = 1
),
metrics as (
    select
        *,
        avg(session_no) over (partition by txn_type, status order by session_no, id rows between 6 preceding and 1 preceding) as session_no_avg_6_prior,
        last_value(status) ignore nulls over (partition by account_id, txn_type order by opened_date, id rows between unbounded preceding and current row) as last_known_status,
        first_value(merchant) ignore nulls over (order by opened_date, id rows between unbounded preceding and current row) as first_known_merchant
    from ranked
),
enriched_transactions as (
    select
        s.*,
        a.balance,
        a.status as account_status
    from metrics s
    left join accounts a
        on s.account_id = a.id
),
ranked_transactions as (
    select *, row_number() over (partition by account_status order by opened_date desc, id) as latest_rank_2
    from enriched_transactions
    qualify latest_rank_2 = 1
)
select
    id,
    merchant,
    session_no,
    account_type,
    opened_date,
    status,
    latest_rank,
    session_no_avg_6_prior,
    balance
from ranked_transactions
