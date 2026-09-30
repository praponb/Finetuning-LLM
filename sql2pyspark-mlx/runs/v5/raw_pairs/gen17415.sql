with filtered as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        date_trunc('MONTH', opened_date) as opened_date_period,
        coalesce(status, 'UNKNOWN') as status_clean
    from accounts
    where opened_date > date '2023-08-01'
      and status in ('CLOSED', 'FROZEN')
),
kept as (
    select s.*
    from filtered s
    where exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.merchant in ('Amazon', 'Lazada')
        )
),
latest_match as (
    select
        s.id,
        s.balance,
        s.status,
        s.opened_date_period,
        s.status_clean,
        x.merchant,
        x.amount
    from kept s
    join lateral (
            select c.merchant, c.amount
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date_period
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    select
        *,
        rank() over (partition by merchant, status order by amount desc nulls last) as amount_rank,
        avg(amount) over (partition by status order by amount, id rows between 3 preceding and 1 preceding) as amount_avg_3_prior,
        ntile(10) over (partition by status order by amount desc, id) as amount_bucket,
        dense_rank() over (partition by merchant order by balance desc) as balance_dense_rank,
        dense_rank() over (partition by status order by amount nulls last) as amount_dense_rank
    from latest_match
),
not_flagged as (
    select s.*
    from scored s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time >= s.opened_date_period
        )
),
metrics as (
    select
        id,
        status,
        status_clean,
        merchant,
        amount_bucket,
        balance_dense_rank,
        ntile(5) over (partition by merchant order by amount_dense_rank desc, id) as amount_dense_rank_bucket,
        sum(amount_bucket) over (partition by status) as status_sum_amount_bucket,
        lead(amount_avg_3_prior, 1) over (partition by status order by amount_avg_3_prior, id) as next_amount_avg_3_prior,
        round(amount_avg_3_prior / sum(amount_avg_3_prior) over (partition by merchant, status), 4) as amount_avg_3_prior_share
    from not_flagged
    where amount < 3800
),
scored_accounts as (
    select
        id,
        status,
        balance_dense_rank,
        amount_avg_3_prior_share,
        lead(status_sum_amount_bucket) over w_part as next_status_sum_amount_bucket,
        lag(status_sum_amount_bucket) over w_part as prev_status_sum_amount_bucket,
        lag(amount_dense_rank_bucket) over w_part as prev_amount_dense_rank_bucket,
        row_number() over w_part as row_seq,
        ntile(4) over w_grp as next_amount_avg_3_prior_bucket
    from metrics
    window w_part as (partition by merchant order by amount_dense_rank_bucket, id), w_grp as (partition by merchant order by next_amount_avg_3_prior desc, id)
)
select
    id,
    status,
    balance_dense_rank,
    prev_status_sum_amount_bucket,
    row_seq,
    next_amount_avg_3_prior_bucket,
    round(prev_status_sum_amount_bucket / sum(prev_status_sum_amount_bucket) over (partition by status), 4) as prev_status_sum_amount_bucket_share,
    lead(row_seq, 2) over (partition by status order by amount_avg_3_prior_share, id) as next_row_seq,
    last_value(balance_dense_rank) ignore nulls over (partition by status order by amount_avg_3_prior_share, id rows between unbounded preceding and current row) as last_known_balance_dense_rank
from scored_accounts
