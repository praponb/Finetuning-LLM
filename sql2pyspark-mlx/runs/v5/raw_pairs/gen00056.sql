with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, case when balance >= 78000 then 'High' when balance >= 26000 then 'Medium' else 'Low' end as balance_tier
    from accounts
    where customer_id is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time <= s.opened_date
        )
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, case when status = 'CLOSED' then 1 else 0 end as is_closed
    from eligible
    where opened_date > date '2023-09-11'
      and account_type != 'CREDIT'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        account_type,
        opened_date,
        ntile(10) over (partition by account_type, customer_id order by balance desc, id) as balance_bucket,
        sum(is_closed) over (partition by account_type order by is_closed, id rows between unbounded preceding and current row) as running_is_closed
    from base
)
-- Multi-level aggregation
select
    case when running_is_closed >= 371 then 'LARGE' else 'SMALL' end as running_is_closed_tier,
    count_if(account_type = 'SAVINGS') as savings_events,
    count(distinct account_type) as distinct_account_types,
    round(avg(running_is_closed), 2) as avg_running_is_closed,
    round(avg(balance_bucket), 2) as avg_balance_bucket,
    sort_array(collect_set(account_type)) as account_type_list,
    percentile_approx(running_is_closed, array(0.25, 0.5, 0.75)) as running_is_closed_quartiles
from enriched
group by 1
