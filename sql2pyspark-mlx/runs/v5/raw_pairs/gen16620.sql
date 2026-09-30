with filtered as (
    select
        id,
        account_id,
        txn_type,
        amount,
        concat(txn_type, ', ', merchant) as txn_type_merchant
    from transactions
    where txn_time >= timestamp '2023-06-24 14:00:00'
      and merchant is not null
),
joined as (
    select
        a.*,
        p2.balance,
        p2.account_type
    from filtered a
    inner join accounts p2
        on a.account_id = p2.id
),
with_dims as (
    select
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.balance,
        s.account_type,
        p.account_type as account_account_type,
        p.balance as account_balance,
        p.status
    from joined s
    inner join accounts p
        on s.account_id = p.id
    where p.status = 'FROZEN'
),
enriched as (
    select
        id,
        txn_type,
        balance,
        account_type,
        account_account_type,
        status,
        count(balance) over w_hist as running_count_balance,
        max(balance) over w_hist as running_max_balance,
        avg(balance) over w_hist as running_avg_balance,
        lag(amount, 1, 0) over w_recent as prev_amount
    from with_dims
    window w_hist as (partition by account_id, account_account_type order by amount, id rows between unbounded preceding and current row), w_recent as (order by balance, id)
)
select
    account_type,
    case when balance >= 81000 then 'PREMIUM' else 'STANDARD' end as balance_tier,
    status,
    sum(case when status = 'OPEN' then balance else 0 end) as open_balance,
    approx_percentile(running_count_balance, 0.5) as running_count_balance_p50,
    round(sum(running_count_balance) / nullif(count(case when status = 'OPEN' then 1 end), 0), 2) as running_count_balance_per_open,
    grouping_id() as grouping_id
from enriched
group by grouping sets ((account_type, case when balance >= 81000 then 'PREMIUM' else 'STANDARD' end, status), (account_type, case when balance >= 81000 then 'PREMIUM' else 'STANDARD' end))
order by running_count_balance_p50 desc
