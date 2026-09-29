with valid as (
    -- Keep recent, valid rows
    select
        id,
        account_type,
        balance,
        status,
        last_day(opened_date) as opened_date_month_end,
        coalesce(status, 'UNKNOWN') as status_clean
    from accounts
    where customer_id is not null
      and status is not distinct from 'OPEN'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.is_flagged = false
        )
),
not_flagged_accounts as (
    -- Semi-join via a correlated subquery
    select s.*
    from not_flagged s
    where exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time <= s.opened_date_month_end
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by status order by opened_date_month_end desc) as latest_rank
        from not_flagged_accounts
    ) t
    where latest_rank = 1
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time < s.opened_date_month_end
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        account_type,
        balance,
        opened_date_month_end,
        sum(balance) over (order by opened_date_month_end, id rows between unbounded preceding and current row) as cumulative_balance,
        ntile(10) over (partition by status, account_type order by balance desc, id) as balance_bucket,
        ntile(4) over (partition by status, account_type order by balance desc, id) as balance_bucket_2
    from eligible
),
metrics as (
    -- Ranking and gap features
    select
        id,
        account_type,
        balance,
        opened_date_month_end,
        cumulative_balance,
        balance_bucket_2,
        lag(balance, 1, 0) over (partition by account_type order by cumulative_balance, id) as prev_balance,
        last_value(cumulative_balance, true) over (partition by account_type order by cumulative_balance, id rows between unbounded preceding and current row) as last_known_cumulative_balance
    from with_windows
),
summary as (
    -- Grouped metrics
    select
        case when prev_balance >= 81000 then 'PREMIUM' else 'STANDARD' end as prev_balance_tier,
        account_type,
        count(*) as num_rows,
        count(*) filter (where account_type = 'CREDIT') as credit_rows
    from metrics
    group by prev_balance_tier, account_type
    having num_rows > 1
)
select
    prev_balance_tier,
    account_type,
    num_rows,
    case when account_type rlike '^(SAVINGS|CHECKING)$' then 'CORE' else 'OTHER_ZONE' end as account_type_zone
from summary;
