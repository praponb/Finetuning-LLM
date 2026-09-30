with joined as (
    -- Attach customers attributes
    select
        s.*,
        c.email,
        c.country
    from subscriptions s
    inner join customers c
        on s.customer_id = c.id
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by customer_id order by end_date desc nulls last, id asc) as dedup_rank
        from joined
    ) x
    where dedup_rank = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        round(monthly_fee / sum(monthly_fee) over (partition by plan), 4) as monthly_fee_share,
        ntile(3) over (partition by plan order by monthly_fee desc, id asc) as monthly_fee_bucket
    from latest
    where dedup_rank <> 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        email,
        country,
        monthly_fee_share,
        substring(email, 1, 2) as email_prefix
    from enriched
    where start_date > date '2023-08-13'
      and country in ('UK', 'DE')
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        monthly_fee,
        status,
        email,
        country,
        monthly_fee_share,
        email_prefix,
        round(monthly_fee / sum(monthly_fee) over (partition by status), 4) as monthly_fee_share_2,
        round(monthly_fee / sum(monthly_fee) over (partition by plan, customer_id), 4) as monthly_fee_share_3,
        row_number() over (partition by status order by monthly_fee_share asc, id asc) as event_seq
    from valid
),
with_windows_subscriptions as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        status,
        email,
        country,
        monthly_fee_share,
        email_prefix,
        monthly_fee_share_2,
        monthly_fee_share_3,
        event_seq,
        last_value(country) ignore nulls over w_running as last_known_country,
        last_value(country, true) over w_grp as last_known_country_2,
        lag(monthly_fee_share_3, 1, 0) over (partition by customer_id order by monthly_fee asc, id asc) as prev_monthly_fee_share_3
    from with_windows
    window w_running as (order by monthly_fee_share asc, id asc rows between unbounded preceding and current row), w_grp as (partition by status order by event_seq asc, id asc rows between unbounded preceding and current row)
),
latest_subscriptions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status, customer_id order by monthly_fee_share desc nulls first, id asc) as dedup_rank
    from with_windows_subscriptions
    qualify dedup_rank = 1
)
-- Grouped metrics
select
    status,
    sum(case when country = 'DE' then monthly_fee_share else 0 end) as de_monthly_fee_share,
    round(avg(monthly_fee_share_3), 2) as avg_monthly_fee_share_3
from latest_subscriptions
group by status
