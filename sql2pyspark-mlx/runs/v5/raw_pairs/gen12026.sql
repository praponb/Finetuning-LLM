with valid as (
    -- Basic cleansing and defaults
    select *, substring(name, 1, 2) name_prefix
    from customers
    where signup_date > date '2023-04-04'
      and segment is not null
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.amount,
        top1.status
    from valid s
    left join lateral (
            select c.amount, c.status
            from orders c
            where c.customer_id = s.id
              and c.order_date <= s.signup_date
            order by c.order_date desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from orders ch
            where ch.customer_id = s.id
              and ch.order_date >= s.signup_date
        )
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        name,
        country,
        city,
        segment,
        age,
        name_prefix,
        amount
    from (
        select *, row_number() over (partition by city order by amount desc nulls first, id) as row_num
        from kept
    ) ranked
    where row_num = 1
),
with_latest as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.country,
        s.city,
        s.age,
        x.account_account_type
    from deduped s
    join lateral (
            select c.account_type as account_account_type
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) x on true
),
filtered as (
    -- Keep recent, valid rows
    select *, case when age >= 60 then 'High' when age >= 41 then 'Medium' else 'Low' end age_level
    from with_latest
    where city is not null
),
base as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(account_account_type, 'unassigned') account_account_type_filled,
        coalesce(account_account_type, 'N/A') resolved_account_account_type
    from filtered
    where country not in ('TH', 'JP')
)
-- Aggregate with conditional metrics
select
    account_account_type,
    round(avg(age), 2) as avg_age,
    count(case when city = 'London' then 1 end) as london_count
from base
group by account_account_type
order by account_account_type desc nulls first;
