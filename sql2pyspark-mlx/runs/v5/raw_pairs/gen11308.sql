with valid as (
    -- Keep recent, valid rows
    select
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        round(credit_limit * 1.15, 2) as adjusted_credit_limit,
        cast(is_active as int) as is_active_flag
    from customers
    where signup_date >= date '2023-05-16'
      and segment is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        email,
        city,
        credit_limit,
        adjusted_credit_limit,
        avg(is_active_flag) over w_grp as is_active_flag_avg_3,
        lag(adjusted_credit_limit, 1) over (partition by segment, city order by is_active_flag asc, id asc) as prev_adjusted_credit_limit
    from valid
    where name like 'Ca%'
    window w_grp as (partition by city order by adjusted_credit_limit asc, id asc rows between 3 preceding and current row)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        email,
        city,
        credit_limit,
        prev_adjusted_credit_limit
    from enriched
    qualify row_number() over (partition by city order by prev_adjusted_credit_limit desc nulls first, id asc) = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        prev_adjusted_credit_limit,
        stddev_samp(prev_adjusted_credit_limit) over (partition by city) as city_stddev_samp_prev_adjusted_credit_limit,
        lead(prev_adjusted_credit_limit, 1, 0) over (partition by city order by prev_adjusted_credit_limit asc, id asc) as next_prev_adjusted_credit_limit,
        last_value(city, true) over (partition by city order by credit_limit asc, id asc rows between unbounded preceding and current row) as last_known_city,
        avg(credit_limit) over (order by prev_adjusted_credit_limit asc, id asc rows between 2 preceding and 1 preceding) as credit_limit_avg_2_prior
    from latest
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from orders c
            where c.customer_id = s.id
              and c.status = 'PENDING'
        )
),
latest_match as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.prev_adjusted_credit_limit,
        s.city_stddev_samp_prev_adjusted_credit_limit,
        s.last_known_city,
        top1.balance
    from not_flagged s
    left join lateral (
            select c.balance
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) top1 on true
),
stats as (
    -- Grouped metrics
    select
        case when balance >= 80000 then 'PREMIUM' else 'STANDARD' end as balance_tier,
        case when prev_adjusted_credit_limit >= 76 then 'LARGE' else 'SMALL' end as prev_adjusted_credit_limit_tier,
        approx_percentile(prev_adjusted_credit_limit, array(0.25, 0.5, 0.75)) as prev_adjusted_credit_limit_quartiles,
        count(*) as num_rows,
        percentile_approx(city_stddev_samp_prev_adjusted_credit_limit, 0.95) as city_stddev_samp_prev_adjusted_credit_limit_p95,
        percentile_approx(prev_adjusted_credit_limit, array(0.25, 0.5, 0.75)) as prev_adjusted_credit_limit_quartiles_2,
        count(*) as num_rows_2
    from latest_match
    group by case when balance >= 80000 then 'PREMIUM' else 'STANDARD' end, case when prev_adjusted_credit_limit >= 76 then 'LARGE' else 'SMALL' end
)
select
    balance_tier,
    prev_adjusted_credit_limit_tier,
    prev_adjusted_credit_limit_quartiles,
    city_stddev_samp_prev_adjusted_credit_limit_p95,
    prev_adjusted_credit_limit_quartiles_2,
    num_rows_2
from stats;
