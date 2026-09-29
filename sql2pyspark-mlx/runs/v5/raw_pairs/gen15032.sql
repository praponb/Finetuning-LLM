with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when status = 'OPEN' then 1 else 0 end as is_open,
        coalesce(status, 'UNKNOWN') as resolved_status
    from accounts
    where balance <= 18000
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(is_open) over (partition by status) as status_avg_is_open,
        cume_dist() over (order by is_open desc nulls last) as is_open_cume_dist,
        lag(balance, 1) over (partition by customer_id order by opened_date, id) as prev_balance
    from valid
    where account_type = 'CHECKING'
),
scored_accounts as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        account_type,
        balance,
        resolved_status,
        status_avg_is_open,
        prev_balance,
        ntile(4) over (partition by customer_id order by prev_balance, id) as prev_balance_bucket,
        avg(status_avg_is_open) over (partition by status, customer_id) as status_avg_status_avg_is_open
    from scored
),
ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by account_type order by prev_balance_bucket desc) as rank_in_group
    from scored_accounts
    qualify rank_in_group = 1
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by account_type order by rank_in_group desc, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from deduped s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.merchant = '7-Eleven'
        )
)
select
    id,
    customer_id,
    balance,
    prev_balance,
    prev_balance_bucket,
    status_avg_status_avg_is_open,
    rank_in_group,
    latest_rank
from kept
order by prev_balance desc nulls last
