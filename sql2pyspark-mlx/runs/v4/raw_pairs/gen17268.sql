with cleaned as (
    select
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        if(is_active, 'Yes', 'No') as is_active_label,
        coalesce(city, 'OTHER') as city_filled
    from customers
    where signup_date >= to_date('2024-01-10')
),
deduped as (
    select *
    from (
        select *, rank() over (partition by country order by signup_date desc nulls last) as rank_in_group
        from cleaned
    ) t
    where rank_in_group = 1
),
scored as (
    select
        *,
        lag(age, 2) over (order by signup_date, id) as prev_age,
        corr(credit_limit, age) over (partition by country) as corr_credit_limit_age,
        corr(credit_limit, age) over (partition by country) as corr_credit_limit_age_2
    from deduped
),
eligible as (
    select s.*
    from scored s
    where not exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
        )
),
with_windows as (
    select
        *,
        sum(age) over (partition by segment order by rank_in_group, id rows between unbounded preceding and current row) as running_age,
        lead(corr_credit_limit_age, 1) over (partition by segment, country order by credit_limit, id) as next_corr_credit_limit_age
    from eligible
),
latest as (
    select *, rank() over (partition by segment order by next_corr_credit_limit_age) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
)
select
    id,
    name,
    email,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    is_active_label,
    city_filled,
    rank_in_group,
    prev_age,
    corr_credit_limit_age,
    corr_credit_limit_age_2,
    running_age,
    next_corr_credit_limit_age,
    dedup_rank,
    dense_rank() over (partition by segment order by prev_age) as prev_age_dense_rank,
    first_value(next_corr_credit_limit_age, true) over (partition by country, segment order by dedup_rank, id rows between unbounded preceding and current row) as first_known_next_corr_credit_limit_age
from latest
