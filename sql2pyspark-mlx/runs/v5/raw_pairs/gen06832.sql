with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from customers s
    where not exists (
            select 1
            from sessions x
            where x.customer_id = s.id
              and x.country = 'US'
        )
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        corr(credit_limit, age) over (partition by city) as corr_credit_limit_age,
        corr(credit_limit, age) over (partition by segment, country) as corr_credit_limit_age_2,
        round(credit_limit / sum(credit_limit) over (partition by city), 4) as credit_limit_share
    from eligible
),
scored as (
    -- Running and moving aggregates
    select
        id,
        segment,
        signup_date,
        corr_credit_limit_age_2,
        corr(age, credit_limit) over (partition by city) as corr_age_credit_limit,
        corr(credit_limit, age) over w_recent as corr_credit_limit_age_3,
        sum(credit_limit_share) over w_hist as cumulative_credit_limit_share
    from metrics
    where corr_credit_limit_age_2 >= 0.1
    window w_recent as (partition by country), w_hist as (order by credit_limit_share asc, id asc rows between unbounded preceding and current row)
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.signup_date,
        s.corr_age_credit_limit,
        s.corr_credit_limit_age_3,
        x.balance
    from scored s
    join lateral (
            select c.balance
            from accounts c
            where c.customer_id = s.id
              and c.opened_date <= s.signup_date
            order by c.opened_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    corr_age_credit_limit,
    corr_credit_limit_age_3,
    balance
from with_last
order by corr_age_credit_limit desc nulls first, id asc;
