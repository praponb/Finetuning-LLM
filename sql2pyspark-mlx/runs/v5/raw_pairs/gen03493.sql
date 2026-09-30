with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        lag(salary, 1, 0) over (order by hire_date, id) as prev_salary,
        lead(salary, 2) over (partition by dept_id order by salary, id) as next_salary,
        avg(salary) over (partition by dept_id order by salary, id rows between 6 preceding and current row) as salary_avg_6
    from employees
    where is_remote = true
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(10) over (partition by job_title order by prev_salary, id) as prev_salary_bucket,
        first_value(job_title) ignore nulls over (partition by job_title order by prev_salary, id rows between unbounded preceding and current row) as first_known_job_title,
        row_number() over (partition by job_title order by salary, id) as event_seq
    from enriched
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        salary,
        hire_date,
        prev_salary,
        event_seq,
        event_seq + next_salary as event_seq_plus_next_salary,
        coalesce(next_salary, 0) as next_salary_filled
    from with_windows
    where length(email) > 12
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        salary,
        hire_date,
        prev_salary,
        event_seq,
        event_seq_plus_next_salary,
        next_salary_filled,
        case when next_salary_filled >= 83 then 'High' when next_salary_filled >= 17 then 'Medium' else 'Low' end as next_salary_filled_tier
    from base
    where prev_salary <= 140000
),
matched_keys as (
    -- Set difference on keys
    select id
    from filtered
    except distinct
    select id
    from employees
    where is_remote
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join matched_keys k on s.id = k.id
)
-- Multi-level aggregation
select
    case when event_seq_plus_next_salary >= 84 then 'LARGE' else 'SMALL' end as event_seq_plus_next_salary_tier,
    approx_percentile(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles,
    round(avg(next_salary_filled), 2) as avg_next_salary_filled,
    count(distinct id) as distinct_ids
from kept_rows
group by event_seq_plus_next_salary_tier
order by distinct_ids desc nulls last
