with latest_match as (
    select
        s.id,
        s.customer_id,
        s.account_type,
        s.opened_date,
        s.status,
        x.txn_time,
        x.merchant
    from accounts s
    join lateral (
            select c.txn_time, c.merchant
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
gaps as (
    select *, case when txn_time - lag(txn_time) over (partition by account_type order by txn_time, id) > interval 14 days then 1 else 0 end as session_start_flag
    from latest_match
),
sessionized as (
    select
        id,
        account_type,
        opened_date,
        status,
        txn_time,
        merchant,
        sum(session_start_flag) over (partition by account_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
cleaned as (
    select *, lower(merchant) as merchant_lower
    from sessionized
    where opened_date > date '2023-06-27'
      and merchant is not null
),
with_latest as (
    select
        s.id,
        s.account_type,
        s.opened_date,
        s.txn_time,
        s.merchant_lower,
        x.transaction_txn_time,
        x.amount
    from cleaned s
    left join lateral (
            select c.txn_time as transaction_txn_time, c.amount
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
with_last as (
    select s.*, top1.transaction_amount
    from with_latest s
    left join lateral (
            select c.amount as transaction_amount
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    account_type,
    opened_date,
    txn_time,
    merchant_lower,
    transaction_txn_time,
    amount,
    transaction_amount,
    case when account_type rlike '^(SAVINGS|LOAN)$' then 'GROUP_A' when account_type rlike '^(CHECKING|CREDIT)$' then 'PRIORITY' else 'CORE' end as account_type_zone
from with_last;
