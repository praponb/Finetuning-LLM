with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when status in ('FROZEN', 'OPEN') then 'Core' else 'Other' end as status_group,
        coalesce(status, 'NONE') as resolved_status
    from accounts
    where status is not null
),
with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.merchant,
        latest.transaction_txn_type
    from cleaned s
    join lateral (
            select c.merchant, c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        status_group,
        resolved_status,
        transaction_txn_type,
        avg(balance) over w_ordered as balance_avg_6_prior,
        row_number() over (partition by customer_id, transaction_txn_type order by balance desc, id) as row_seq
    from with_latest
    window w_ordered as (partition by customer_id order by balance, id rows between 6 preceding and 1 preceding)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        opened_date,
        lag(balance, 2) over (partition by status, account_type order by row_seq, id) as prev_balance,
        first_value(status, true) over (partition by status order by row_seq, id rows between unbounded preceding and current row) as first_known_status,
        ntile(4) over (partition by transaction_txn_type order by balance_avg_6_prior, id) as balance_avg_6_prior_bucket
    from with_windows
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time < s.opened_date
        )
),
with_latest_accounts as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, top1.transaction_txn_time
    from eligible s
    left join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
totals as (
    -- Grouped metrics
    select
        case when balance_avg_6_prior_bucket >= 8 then 'LARGE' else 'SMALL' end as balance_avg_6_prior_bucket_tier,
        min(transaction_txn_time) as first_seen,
        round(avg(prev_balance), 2) as avg_prev_balance,
        round(avg(balance_avg_6_prior_bucket), 2) as avg_balance_avg_6_prior_bucket,
        count(*) as cnt,
        max(transaction_txn_time) as last_transaction_txn_time,
        percentile_approx(balance_avg_6_prior_bucket, 0.9) as balance_avg_6_prior_bucket_p90
    from with_latest_accounts
    group by balance_avg_6_prior_bucket_tier
)
select
    balance_avg_6_prior_bucket_tier,
    first_seen,
    avg_prev_balance,
    last_transaction_txn_time
from totals
