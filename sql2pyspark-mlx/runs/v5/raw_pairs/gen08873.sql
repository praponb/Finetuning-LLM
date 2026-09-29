with filtered as (
    -- Basic cleansing and defaults
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
        coalesce(city, 'Unknown') as city_filled,
        initcap(trim(segment)) as segment_title,
        coalesce(segment, 'N/A') as segment_clean
    from customers
    where signup_date >= to_date('2023-08-25')
      and credit_limit >= 13300
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by segment order by credit_limit desc) as rank_in_group
        from filtered
    ) x
    where rank_in_group = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(credit_limit) over w_recent as running_min_credit_limit,
        count(credit_limit) over w_recent as running_count_credit_limit,
        sum(credit_limit) over w_recent as running_sum_credit_limit
    from ranked
    where email like '%@company.co'
    window w_recent as (partition by segment, city order by rank_in_group, id rows between unbounded preceding and current row)
),
scored as (
    -- Ranking and gap features
    select
        id,
        email,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        segment_clean,
        rank_in_group,
        running_count_credit_limit,
        running_sum_credit_limit,
        row_number() over w_grp as seq_no,
        sum(running_min_credit_limit) over (partition by city order by signup_date, id rows between unbounded preceding and current row) as cumulative_running_min_credit_limit
    from metrics
    window w_grp as (order by signup_date desc, id)
)
select
    id,
    email,
    city,
    signup_date,
    credit_limit,
    age,
    segment_clean,
    rank_in_group,
    seq_no
from scored
