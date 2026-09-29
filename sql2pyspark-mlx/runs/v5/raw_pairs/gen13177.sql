with valid as (
    select *, upper(account_type) as account_type_upper
    from accounts
    where status is distinct from 'OPEN'
      and status = 'OPEN'
),
with_last as (
    select s.*, x.transaction_txn_time
    from valid s
    left join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
with_latest as (
    select
        s.id,
        s.customer_id,
        s.account_type,
        s.status,
        s.transaction_txn_time,
        top1.txn_type
    from with_last s
    left join lateral (
            select c.txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
joined as (
    select
        src.id,
        src.customer_id,
        src.status,
        src.transaction_txn_time,
        src.txn_type,
        p.email,
        p.signup_date
    from with_latest src
    left join customers p
        on src.customer_id = p.id
),
deduped as (
    select *, row_number() over (partition by txn_type, customer_id order by signup_date desc nulls first, id) as latest_rank
    from joined
    qualify latest_rank = 1
)
select
    date_trunc('week', transaction_txn_time) as week_start,
    status,
    count_if(txn_type = 'REFUND') as refund_events,
    count(*) as cnt,
    count_if(txn_type = 'FEE') as fee_events,
    grouping(date_trunc('week', transaction_txn_time)) as grouping_level
from deduped
group by rollup (date_trunc('week', transaction_txn_time), status)
order by refund_events desc nulls last;
