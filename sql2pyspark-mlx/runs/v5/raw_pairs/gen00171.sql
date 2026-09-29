with metrics as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        ntile(10) over (partition by location order by budget, id) as budget_bucket,
        first_value(location) ignore nulls over (partition by location order by budget, id rows between unbounded preceding and current row) as first_known_location,
        cume_dist() over (partition by location, dept_name order by budget desc) as budget_cume_dist
    from departments
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        dept_name,
        budget_cume_dist,
        last_value(budget_bucket) ignore nulls over w_part as last_known_budget_bucket,
        ntile(3) over (partition by dept_name order by budget_cume_dist, id) as budget_cume_dist_bucket,
        first_value(budget_bucket) ignore nulls over w_part as first_known_budget_bucket,
        lag(budget_cume_dist, 2) over (partition by dept_name order by budget_bucket, id) as prev_budget_cume_dist,
        last_value(budget_bucket) ignore nulls over w_part as last_known_budget_bucket_2
    from metrics
    window w_part as (partition by dept_name order by budget_bucket, id rows between unbounded preceding and current row)
),
scored as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        budget_cume_dist,
        last_known_budget_bucket,
        budget_cume_dist_bucket,
        first_known_budget_bucket,
        prev_budget_cume_dist,
        lag(budget_cume_dist) over (partition by dept_name order by first_known_budget_bucket, id) as prev_budget_cume_dist_2,
        lead(prev_budget_cume_dist) over (partition by dept_name order by first_known_budget_bucket, id) as next_prev_budget_cume_dist,
        lag(prev_budget_cume_dist) over (partition by dept_name order by first_known_budget_bucket, id) as prev_prev_budget_cume_dist,
        row_number() over (partition by dept_name order by first_known_budget_bucket, id) as event_seq
    from with_windows
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select s.*, latest.employee_hire_date
    from scored s
    left join lateral (
            select c.hire_date as employee_hire_date
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
scored_departments as (
    -- Ranking and gap features
    select
        id,
        dept_name,
        budget_cume_dist,
        budget_cume_dist_bucket,
        first_known_budget_bucket,
        prev_budget_cume_dist,
        prev_budget_cume_dist_2,
        next_prev_budget_cume_dist,
        prev_prev_budget_cume_dist,
        event_seq,
        employee_hire_date,
        max(budget_cume_dist) over (partition by dept_name) as dept_name_max_budget_cume_dist,
        lead(prev_budget_cume_dist_2, 1) over w_ordered as next_prev_budget_cume_dist_2,
        avg(first_known_budget_bucket) over (order by budget_cume_dist, id rows between 5 preceding and 1 preceding) as first_known_budget_bucket_avg_5_prior,
        ntile(4) over (partition by dept_name order by prev_budget_cume_dist, id) as prev_budget_cume_dist_bucket
    from with_latest
    window w_ordered as (partition by dept_name order by event_seq, id)
)
-- Window metrics over several different partitions
select
    id,
    dept_name,
    budget_cume_dist_bucket,
    first_known_budget_bucket,
    prev_budget_cume_dist,
    prev_budget_cume_dist_2,
    next_prev_budget_cume_dist,
    event_seq,
    first_known_budget_bucket_avg_5_prior,
    round(prev_budget_cume_dist_bucket / sum(prev_budget_cume_dist_bucket) over (partition by dept_name), 4) as prev_budget_cume_dist_bucket_share,
    sum(next_prev_budget_cume_dist) over w_recent as cumulative_next_prev_budget_cume_dist,
    row_number() over (partition by dept_name order by next_prev_budget_cume_dist_2 desc, id) as event_seq_2
from scored_departments
window w_recent as (order by prev_prev_budget_cume_dist, id rows between unbounded preceding and current row)
