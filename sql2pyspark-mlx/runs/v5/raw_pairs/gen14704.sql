with cleaned as (
    select
        *,
        concat_ws(',', merchant, txn_type) as merchant_txn_type_key,
        coalesce(merchant, 'NONE') as merchant_clean
    from transactions
    where txn_time > timestamp '2023-07-20 15:00:00'
),
scored as (
    select
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        merchant_txn_type_key,
        cume_dist() over (partition by merchant order by amount desc) as amount_cume_dist,
        ntile(5) over (partition by txn_type order by amount desc, id) as amount_bucket,
        avg(amount) over (partition by txn_type order by amount, id rows between 2 preceding and current row) as amount_avg_2
    from cleaned
),
latest as (
    select
        id,
        txn_type,
        merchant,
        merchant_txn_type_key,
        amount_bucket
    from (
        select *, row_number() over (partition by merchant order by amount_cume_dist, id) as row_num
        from scored
    ) t
    where row_num = 1
),
scored_transactions as (
    select
        *,
        round(amount_bucket / sum(amount_bucket) over (partition by txn_type), 4) as amount_bucket_share,
        rank() over (partition by merchant, txn_type order by amount_bucket desc nulls first) as amount_bucket_rank
    from latest
),
latest_transactions as (
    select *
    from (
        select *, row_number() over (partition by merchant order by amount_bucket nulls last, id) as latest_rank
        from scored_transactions
    ) t
    where latest_rank = 1
)
select
    merchant,
    txn_type,
    count(case when txn_type = 'CREDIT' then 1 end) as credit_count,
    max(amount_bucket) as max_amount_bucket,
    avg(amount_bucket_rank) filter (where txn_type = 'DEBIT') as debit_avg_amount_bucket_rank,
    approx_percentile(amount_bucket, array(0.25, 0.5, 0.75)) as amount_bucket_quartiles,
    count(*) as cnt
from latest_transactions
group by 1, 2
order by credit_count desc
