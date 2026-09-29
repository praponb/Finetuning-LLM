with deduped as (
    select *
    from accounts
    qualify row_number() over (partition by status order by opened_date desc, id) = 1
),
valid as (
    select
        id,
        customer_id,
        balance,
        opened_date,
        substring(account_type, 1, 3) as account_type_prefix,
        upper(account_type) as account_type_upper
    from deduped
    where opened_date > to_date('2023-10-29')
      and customer_id is not null
      and opened_date < '2025-09-18'
),
metrics as (
    select
        id,
        customer_id,
        balance,
        opened_date,
        account_type_prefix,
        account_type_upper,
        avg(balance) over w_hist as balance_avg_5_prior,
        row_number() over (order by balance, id) as seq_no,
        ntile(3) over (partition by customer_id order by balance, id) as balance_bucket,
        ntile(3) over (partition by customer_id order by balance desc, id) as balance_bucket_2,
        row_number() over (order by balance desc, id) as seq_no_2
    from valid
    window w_hist as (partition by customer_id order by opened_date, id rows between 5 preceding and 1 preceding)
),
kept as (
    select s.*
    from metrics s
    where exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.merchant in ('7-Eleven', 'Lazada', 'Shell')
        )
),
not_flagged as (
    select s.*
    from kept s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time < s.opened_date
        )
)
select
    id,
    customer_id,
    balance,
    opened_date,
    account_type_prefix,
    account_type_upper,
    balance_avg_5_prior,
    seq_no,
    balance_bucket,
    balance_bucket_2,
    seq_no_2,
    sum(balance_bucket_2) over w_grp as running_balance_bucket_2,
    cume_dist() over (partition by customer_id order by balance_bucket_2 desc nulls last) as balance_bucket_2_cume_dist,
    avg(balance_bucket) over (partition by customer_id) as customer_id_avg_balance_bucket,
    sum(seq_no) over (order by balance_bucket_2, id rows between unbounded preceding and current row) as cumulative_seq_no
from not_flagged
window w_grp as (order by balance_bucket, id rows between unbounded preceding and current row);
