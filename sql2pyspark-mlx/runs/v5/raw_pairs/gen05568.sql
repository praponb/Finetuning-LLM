with matched_keys as (
    -- Set difference on keys
    select id
    from transactions
    except all
    select id
    from transactions
    where amount <= 2800
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from transactions s
    inner join matched_keys k on s.id = k.id
),
enriched as (
    -- Attach accounts attributes
    select
        a.*,
        p2.balance,
        p2.status
    from reconciled a
    left join accounts p2
        on a.account_id = p2.id
    where p2.customer_id = 17
),
with_dims as (
    -- Attach accounts attributes
    select
        e.*,
        p.balance as account_balance,
        p.account_type
    from enriched e
    left join accounts p
        on e.account_id = p.id
),
with_windows as (
    -- Running and moving aggregates
    select
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        balance,
        status,
        account_balance,
        account_type,
        round(amount / sum(amount) over (partition by merchant, account_id), 4) as amount_share,
        sum(account_balance) over (partition by account_type order by cast(txn_time as bigint) range between 604800 preceding and current row) as account_balance_sum_7d,
        sum(balance) over (partition by txn_type, account_type order by cast(txn_time as bigint) range between 604800 preceding and current row) as balance_sum_7d,
        avg(balance) over (partition by txn_type order by cast(txn_time as bigint) range between 2592000 preceding and current row) as balance_avg_30d
    from with_dims
)
select
    txn_time,
    is_flagged,
    balance,
    status,
    account_balance_sum_7d,
    balance_sum_7d,
    balance_avg_30d
from with_windows
order by txn_time desc nulls last
