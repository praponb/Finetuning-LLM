with remaining_keys as (
    select account_id, merchant
    from transactions
    except
    select account_id, merchant
    from transactions
    where merchant = 'Starbucks'
),
reconciled as (
    select s.*
    from transactions s
    inner join remaining_keys k on s.account_id = k.account_id and s.merchant = k.merchant
),
joined as (
    select
        s.*,
        b.status,
        b.opened_date,
        b.account_type
    from reconciled s
    inner join accounts b
        on s.account_id = b.id
    where b.balance <= 32000
),
base as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        status,
        opened_date,
        account_type,
        floor(amount) as amount_whole,
        case when status = 'FROZEN' then 1 else 0 end as is_frozen
    from joined
    where merchant is not null
      and account_id = 35
),
with_dims as (
    select
        e.*,
        d.opened_date as account_opened_date,
        d.balance,
        d.account_type as account_account_type
    from base e
    inner join accounts d
        on e.account_id = d.id
),
filtered as (
    select
        id,
        account_id,
        txn_time,
        is_flagged,
        status,
        amount_whole,
        is_frozen,
        account_opened_date,
        balance,
        account_account_type,
        length(txn_type) as txn_type_length,
        round(is_frozen / amount, 2) as is_frozen_per_amount,
        coalesce(status, 'N/A') as status_clean
    from with_dims
    where account_account_type is distinct from 'CREDIT'
      and account_opened_date <= '2023-11-01'
),
base_transactions as (
    select
        *,
        to_date(txn_time) as txn_time_date,
        coalesce(status, 'DIRECT') as status_filled
    from filtered
    where account_account_type is not null
)
select
    id,
    txn_time,
    is_flagged,
    amount_whole,
    is_frozen,
    balance,
    txn_type_length,
    is_frozen_per_amount,
    status_clean
from base_transactions
order by is_frozen desc nulls last, balance asc
