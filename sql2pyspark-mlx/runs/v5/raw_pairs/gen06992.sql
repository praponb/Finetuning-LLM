with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        email,
        country,
        signup_date,
        is_active,
        credit_limit,
        age,
        case when is_active then 'Yes' else 'No' end as is_active_label
    from customers
    where city is distinct from 'New York'
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by country order by signup_date desc nulls first, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        first_value(credit_limit) ignore nulls over (partition by country order by credit_limit, id rows between unbounded preceding and current row) as first_known_credit_limit,
        avg(credit_limit) over (partition by country order by latest_rank, id rows between 6 preceding and current row) as credit_limit_avg_6,
        round(credit_limit / sum(credit_limit) over (partition by country), 4) as credit_limit_share,
        dense_rank() over (order by credit_limit) as credit_limit_dense_rank
    from latest
),
enriched as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (partition by country order by credit_limit_share desc nulls last) as credit_limit_share_cume_dist,
        ntile(3) over (partition by country order by credit_limit_avg_6, id) as credit_limit_avg_6_bucket
    from scored
),
latest_match as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.session_country,
        top1.device
    from enriched s
    join lateral (
            select c.country as session_country, c.device
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    email,
    country,
    signup_date,
    is_active,
    credit_limit,
    age,
    is_active_label,
    credit_limit_avg_6
from latest_match
order by id
