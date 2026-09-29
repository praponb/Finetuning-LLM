with valid as (
    select
        *,
        case when status = 'OPEN' then 1 else 0 end as is_open,
        coalesce(status, 'UNKNOWN') as resolved_status
    from accounts
    where opened_date > date '2024-04-25'
),
base as (
    select
        *,
        round(is_open * 1.15, 2) as adjusted_is_open,
        cast(is_open as string) as is_open_num
    from valid
    where opened_date >= to_date('2023-08-14')
),
with_last as (
    select
        s.*,
        top1.txn_time,
        top1.transaction_txn_type
    from base s
    left join lateral (
            select c.txn_time, c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from with_last s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time <= s.txn_time
        )
),
scored as (
    select
        *,
        avg(balance) over (partition by transaction_txn_type order by cast(txn_time as bigint) range between 604800 preceding and current row) as balance_avg_7d,
        ntile(3) over (partition by customer_id, status order by balance desc, id) as balance_bucket,
        avg(is_open) over w_running as is_open_avg_7d,
        lead(balance, 1) over (partition by customer_id, transaction_txn_type order by txn_time, id) as next_balance,
        cume_dist() over w_recent as is_open_cume_dist
    from not_flagged
    window w_running as (partition by customer_id order by cast(txn_time as bigint) range between 604800 preceding and current row), w_recent as (partition by customer_id order by is_open desc nulls first)
),
with_gaps as (
    select *, case when lag(txn_time) over (partition by customer_id order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by customer_id order by txn_time, id) + interval '90' day then 1 else 0 end as is_new_burst
    from scored
),
sessionized as (
    select *, sum(is_new_burst) over (partition by customer_id order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
scored_accounts as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        is_open,
        resolved_status,
        adjusted_is_open,
        is_open_num,
        txn_time,
        transaction_txn_type,
        balance_avg_7d,
        balance_bucket,
        is_open_avg_7d,
        next_balance,
        is_open_cume_dist,
        is_new_burst,
        island_id,
        cume_dist() over (partition by customer_id order by is_open_avg_7d nulls last) as is_open_avg_7d_cume_dist,
        stddev_samp(is_open_cume_dist) over (partition by customer_id) as customer_id_stddev_samp_is_open_cume_dist,
        row_number() over (partition by account_type order by island_id desc, id) as row_seq,
        lead(is_open_avg_7d, 2) over (partition by transaction_txn_type order by is_open, id) as next_is_open_avg_7d
    from sessionized
)
select
    *,
    avg(balance_bucket) over (partition by transaction_txn_type order by next_is_open_avg_7d, id rows between 2 preceding and 1 preceding) as balance_bucket_avg_2_prior,
    ntile(5) over (partition by status, account_type order by is_open desc, id) as is_open_bucket,
    lead(next_balance, 1) over (partition by customer_id order by balance_bucket, id) as next_next_balance
from scored_accounts;
