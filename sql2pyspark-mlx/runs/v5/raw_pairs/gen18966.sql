with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from customers s
    where exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date <= s.signup_date
        )
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.account_account_type,
        top1.account_status
    from kept s
    join lateral (
            select c.account_type as account_account_type, c.status as account_status
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(credit_limit / sum(credit_limit) over (partition by account_status), 4) as credit_limit_share,
        ntile(4) over w_recent as age_bucket,
        corr(age, credit_limit) over w_hist as corr_age_credit_limit,
        percent_rank() over w_grp as age_percent_rank,
        ntile(4) over w_recent_2 as age_bucket_2
    from with_last
    window w_recent as (partition by country, city order by age, id), w_hist as (partition by account_account_type), w_grp as (partition by segment order by age nulls first), w_recent_2 as (partition by segment order by age desc, id)
),
kept_customers as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from subscriptions c
            where c.customer_id = s.id
              and c.status != 'PAUSED'
        )
)
-- Grouped metrics
select
    date_trunc('week', signup_date) as week_start,
    count(*) as cnt,
    round(avg(age_bucket_2), 2) as avg_age_bucket_2,
    count(case when account_status = 'FROZEN' then 1 end) as frozen_count,
    count(*) filter (where country = 'UK') as uk_rows
from kept_customers
group by date_trunc('week', signup_date)
having count(*) filter (where country = 'UK') > 1
