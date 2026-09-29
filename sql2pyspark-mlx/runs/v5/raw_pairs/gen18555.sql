with with_last as (
    select
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.status,
        latest.transaction_txn_type
    from accounts s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) latest on true
),
metrics as (
    select
        id,
        customer_id,
        balance,
        opened_date,
        transaction_txn_type,
        stddev_samp(balance) over (partition by account_type) as account_type_stddev_samp_balance,
        avg(balance) over (order by balance, id rows between 3 preceding and current row) as balance_avg_3,
        row_number() over (order by opened_date, id) as row_seq
    from with_last
),
joined as (
    select
        s.id,
        s.customer_id,
        s.balance,
        s.opened_date,
        s.account_type_stddev_samp_balance,
        s.row_seq,
        c.credit_limit
    from metrics s
    join customers c
        on s.customer_id = c.id
),
joined_accounts as (
    select
        a.id,
        a.customer_id,
        a.credit_limit,
        p.segment,
        p.credit_limit as customer_credit_limit
    from joined a
    join customers p
        on a.customer_id = p.id
),
with_latest as (
    select s.*, top1.transaction_merchant
    from joined_accounts s
    left join lateral (
            select c.merchant as transaction_merchant
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
)
select *
from (
    select
        customer_id,
        transaction_merchant,
        credit_limit
    from with_latest
) src
pivot (
    sum(credit_limit) as value_sum,
    count(1) as item_count
    for transaction_merchant in ('Shell' as shell, '7-Eleven' as v_7_eleven)
);
