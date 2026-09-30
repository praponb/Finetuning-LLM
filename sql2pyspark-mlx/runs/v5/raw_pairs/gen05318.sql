with with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, x.transaction_merchant
    from accounts s
    left join lateral (
            select c.merchant as transaction_merchant
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time >= s.opened_date
        )
),
remaining_keys as (
    -- Set difference on keys
    select id, customer_id
    from not_flagged
    except distinct
    select id, customer_id
    from accounts
    where balance > 55000
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join remaining_keys k on s.id = k.id and s.customer_id = k.customer_id
),
base as (
    -- Keep recent, valid rows
    select
        *,
        substring(status, 1, 4) as status_prefix,
        date_add(opened_date, 56) as opened_date_plus_56d,
        coalesce(status, 'NONE') as status_filled
    from kept_rows
    where balance is not null
),
with_dims as (
    -- Attach customers attributes
    select
        a.customer_id,
        a.balance,
        a.opened_date,
        a.transaction_merchant,
        a.status_prefix,
        a.status_filled,
        p.country,
        p.email,
        p.credit_limit
    from base a
    left join customers p
        on a.customer_id = p.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_recent as balance_cume_dist,
        percent_rank() over w_recent_2 as credit_limit_percent_rank,
        round(credit_limit / sum(credit_limit) over (partition by transaction_merchant), 4) as credit_limit_share
    from with_dims
    where customer_id is null
    window w_recent as (partition by country order by balance desc), w_recent_2 as (partition by customer_id order by credit_limit desc nulls first)
),
kept_keys as (
    -- Set intersection on keys
    select customer_id
    from metrics
    intersect
    select customer_id
    from accounts
    where customer_id = 2
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join kept_keys k on s.customer_id = k.customer_id
)
-- Grouped metrics
select
    case when credit_limit_percent_rank >= 0.64 then 'LARGE' else 'SMALL' end as credit_limit_percent_rank_tier,
    count(distinct transaction_merchant) as distinct_transaction_merchants,
    count(*) as event_count,
    stddev_samp(balance) as balance_stddev
from verified
group by rollup (case when credit_limit_percent_rank >= 0.64 then 'LARGE' else 'SMALL' end)
