with filtered as (
    -- Keep recent, valid rows
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        greatest(credit_limit, age) as greatest_credit_limit_age,
        upper(city) as city_upper
    from customers
    where signup_date >= date '2023-03-20'
      and city is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        segment,
        greatest_credit_limit_age,
        city_upper,
        first_value(age) ignore nulls over (partition by country order by credit_limit, id rows between unbounded preceding and current row) as first_known_age,
        dense_rank() over (partition by country order by age desc nulls last) as age_dense_rank,
        first_value(age, true) over (partition by segment order by greatest_credit_limit_age, id rows between unbounded preceding and current row) as first_known_age_2
    from filtered
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from subscriptions c
            where c.customer_id = s.id
              and c.plan not in ('standard', 'enterprise', 'basic')
        )
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select s.*, top1.account_account_type
    from not_flagged s
    left join lateral (
            select c.account_type as account_account_type
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) top1 on true
)
-- Grouped metrics
select
    case when greatest_credit_limit_age >= 65 then 'PREMIUM' else 'STANDARD' end as greatest_credit_limit_age_tier,
    case when age_dense_rank >= 9 then 'LARGE' else 'SMALL' end as age_dense_rank_tier,
    case when first_known_age_2 >= 72 then 'LARGE' else 'SMALL' end as first_known_age_2_tier,
    avg(age_dense_rank) filter (where account_account_type = 'CHECKING') as checking_avg_age_dense_rank,
    avg(first_known_age_2) as avg_first_known_age_2,
    count(case when account_account_type = 'SAVINGS' then 1 end) as savings_count,
    count_if(account_account_type = 'LOAN') as loan_events,
    avg(first_known_age) as avg_first_known_age,
    sum(case when account_account_type = 'CHECKING' then first_known_age else 0 end) as checking_first_known_age
from with_last
group by grouping sets ((case when greatest_credit_limit_age >= 65 then 'PREMIUM' else 'STANDARD' end, case when age_dense_rank >= 9 then 'LARGE' else 'SMALL' end, case when first_known_age_2 >= 72 then 'LARGE' else 'SMALL' end), (case when greatest_credit_limit_age >= 65 then 'PREMIUM' else 'STANDARD' end, case when age_dense_rank >= 9 then 'LARGE' else 'SMALL' end), (case when greatest_credit_limit_age >= 65 then 'PREMIUM' else 'STANDARD' end), ())
having count(case when account_account_type = 'SAVINGS' then 1 end) < 1000
order by checking_avg_age_dense_rank desc
