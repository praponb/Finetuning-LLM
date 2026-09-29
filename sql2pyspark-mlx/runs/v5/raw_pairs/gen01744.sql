with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by segment order by credit_limit desc nulls first) as row_num
    from customers
    qualify row_num = 1
),
ranked_customers as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        credit_limit,
        age
    from (
        select *, rank() over (partition by segment order by signup_date asc nulls first) as latest_rank
        from ranked
    ) ranked
    where latest_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(credit_limit, 1) over (partition by country, segment order by age asc, id asc) as prev_credit_limit,
        max(age) over (partition by segment, country) as segment_max_age,
        sum(age) over (order by age asc, id asc rows between unbounded preceding and current row) as cumulative_age,
        first_value(age) ignore nulls over (partition by segment order by credit_limit asc, id asc rows between unbounded preceding and current row) as first_known_age
    from ranked_customers
    where age < 47
),
metrics_customers as (
    -- Ranking and gap features
    select
        id,
        name,
        email,
        country,
        signup_date,
        age,
        prev_credit_limit,
        cumulative_age,
        first_known_age,
        sum(segment_max_age) over w_grp as running_sum_segment_max_age,
        count(segment_max_age) over w_grp as running_count_segment_max_age,
        avg(segment_max_age) over w_grp as running_avg_segment_max_age
    from metrics
    window w_grp as (partition by segment order by signup_date asc, id asc rows between unbounded preceding and current row)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        email,
        signup_date,
        age,
        prev_credit_limit,
        running_avg_segment_max_age,
        min(running_sum_segment_max_age) over w_hist as last4_min_running_sum_segment_max_age,
        max(running_sum_segment_max_age) over w_hist as last4_max_running_sum_segment_max_age,
        avg(running_sum_segment_max_age) over w_hist as last4_avg_running_sum_segment_max_age,
        round(age / sum(age) over (partition by country), 4) as age_share
    from metrics_customers
    window w_hist as (partition by country order by first_known_age asc, id asc rows between 3 preceding and current row)
)
-- Aggregate with conditional metrics
select
    date_trunc('week', signup_date) as week_start,
    case when age >= 71 then 'HIGH' else 'LOW' end as age_tier,
    avg(prev_credit_limit) as avg_prev_credit_limit,
    bit_and(age) as age_bit_and,
    round(avg(last4_max_running_sum_segment_max_age), 2) as avg_last4_max_running_sum_segment_max_age,
    avg(running_avg_segment_max_age) as avg_running_avg_segment_max_age
from with_windows
group by date_trunc('week', signup_date), case when age >= 71 then 'HIGH' else 'LOW' end;
