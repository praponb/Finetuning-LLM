with filtered as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        trim(txn_type) as clean_txn_type,
        weekofyear(txn_time) as txn_time_weekofyear
    from transactions
    where merchant is not null
),
joined as (
    -- Attach accounts attributes
    select
        src.id,
        src.account_id,
        src.txn_type,
        src.txn_time,
        src.merchant,
        src.is_flagged,
        src.txn_time_weekofyear,
        d.opened_date,
        d.status
    from filtered src
    left join accounts d
        on src.account_id = d.id
),
enriched as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_time,
        is_flagged,
        opened_date,
        status,
        ntile(4) over w_running as txn_time_weekofyear_bucket,
        sum(txn_time_weekofyear) over (partition by merchant) as merchant_sum_txn_time_weekofyear
    from joined
    window w_running as (order by txn_time_weekofyear, id)
),
enriched_transactions as (
    -- Attach accounts attributes
    select
        s.id,
        s.account_id,
        s.opened_date,
        s.status,
        b.status as account_status
    from enriched s
    inner join accounts b
        on s.account_id = b.id
)
-- Ranking and gap features
select
    id,
    opened_date,
    status,
    account_status,
    first_value(status) ignore nulls over (partition by status, account_status order by opened_date, id rows between unbounded preceding and current row) as first_known_status,
    row_number() over (partition by account_status order by opened_date, id) as event_seq,
    last_value(status) ignore nulls over (partition by status order by opened_date, id rows between unbounded preceding and current row) as last_known_status,
    row_number() over (partition by status order by opened_date, id) as row_seq
from enriched_transactions
