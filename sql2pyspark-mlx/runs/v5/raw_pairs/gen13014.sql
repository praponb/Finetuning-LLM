with base as (
    -- Keep recent, valid rows
    select
        *,
        upper(status) as status_upper,
        substring(account_type, 1, 4) as account_type_prefix,
        coalesce(status, 'NONE') as resolved_status
    from accounts
    where opened_date >= date '2023-03-26'
      and customer_id is not null
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.status_upper,
        x.transaction_merchant,
        x.transaction_amount
    from base s
    left join lateral (
            select c.merchant as transaction_merchant, c.amount as transaction_amount
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.amount,
        latest.transaction_txn_type
    from with_last s
    join lateral (
            select c.amount, c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over w_grp as amount_bucket,
        last_value(transaction_txn_type, true) over (partition by transaction_txn_type, transaction_merchant order by transaction_amount asc, id asc rows between unbounded preceding and current row) as last_known_transaction_txn_type,
        ntile(5) over (partition by transaction_txn_type order by amount asc, id asc) as amount_bucket_2,
        round(amount / sum(amount) over (partition by transaction_merchant), 4) as amount_share,
        row_number() over (partition by transaction_merchant, transaction_txn_type order by amount desc, id asc) as event_seq
    from with_latest
    window w_grp as (order by amount asc, id asc)
)
-- Grouped metrics
select
    transaction_merchant,
    count(distinct transaction_txn_type) as distinct_transaction_txn_types,
    approx_percentile(amount_bucket, array(0.25, 0.5, 0.75)) as amount_bucket_quartiles,
    percentile_approx(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    count(*) filter (where transaction_merchant = '7-Eleven') as 7_eleven_rows,
    grouping_id() as grouping_level
from with_windows
group by rollup (transaction_merchant)
