with valid as (
    -- Keep recent, valid rows
    select
        id,
        account_type,
        balance,
        opened_date,
        status,
        round(balance, 2) as balance_rounded,
        add_months(opened_date, 1) as opened_date_plus_months,
        coalesce(status, 'UNKNOWN') as status_filled
    from accounts
    where balance < 69000
      and status is distinct from 'CLOSED'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        account_type,
        balance,
        status,
        ntile(3) over (partition by status, account_type order by balance_rounded desc, id) as balance_rounded_bucket,
        last_value(balance) ignore nulls over (partition by status order by balance, id rows between unbounded preceding and current row) as last_known_balance,
        avg(balance) over (partition by account_type order by opened_date, id rows between 3 preceding and current row) as balance_avg_3
    from valid
),
kept_keys as (
    -- Set intersection on keys
    select account_type
    from scored
    intersect
    select account_type
    from accounts
    where customer_id is null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join kept_keys k on s.account_type = k.account_type
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.amount >= 1600
        )
),
scored_accounts as (
    -- Window metrics over several different partitions
    select
        status,
        last_known_balance,
        dense_rank() over (partition by status order by balance_avg_3 desc nulls last) as balance_avg_3_dense_rank
    from not_flagged
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(last_known_balance, 1) as last_known_balance_rounded,
        coalesce(status, 'UNKNOWN') as resolved_status
    from scored_accounts
    where status is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        status,
        last_known_balance,
        balance_avg_3_dense_rank,
        min(balance_avg_3_dense_rank) over w_recent as status_min_balance_avg_3_dense_rank,
        min(last_known_balance_rounded) over w_recent as status_min_last_known_balance_rounded,
        round(balance_avg_3_dense_rank / sum(balance_avg_3_dense_rank) over (partition by status), 4) as balance_avg_3_dense_rank_share,
        percent_rank() over (partition by status order by last_known_balance nulls last) as last_known_balance_percent_rank,
        percent_rank() over (partition by status order by balance_avg_3_dense_rank desc nulls first) as balance_avg_3_dense_rank_percent_rank
    from cleaned
    where last_known_balance >= 35000
    window w_recent as (partition by status)
)
select
    last_known_balance,
    balance_avg_3_dense_rank,
    status_min_last_known_balance_rounded,
    balance_avg_3_dense_rank_share,
    balance_avg_3_dense_rank_percent_rank
from with_windows
order by last_known_balance desc nulls first;
