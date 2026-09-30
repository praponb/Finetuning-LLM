with valid as (
    -- Basic cleansing and defaults
    select
        *,
        lower(email) as email_lower,
        coalesce(level, 'N/A') as level_filled
    from employees
    where hire_date > to_date('2024-02-19')
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(10) over (partition by manager_id order by salary desc, id) as salary_bucket,
        avg(salary) over (order by salary, id rows between 2 preceding and 1 preceding) as salary_avg_2_prior,
        row_number() over (order by hire_date desc, id) as seq_no,
        sum(salary) over (partition by dept_id, job_title order by hire_date, id rows between unbounded preceding and current row) as running_salary
    from valid
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by job_title order by salary, id) as rank_in_group
        from enriched
    ) x
    where rank_in_group = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        last_value(running_salary) ignore nulls over (partition by manager_id, level order by salary_avg_2_prior, id rows between unbounded preceding and current row) as last_known_running_salary,
        avg(salary_bucket) over (order by salary_avg_2_prior, id rows between 2 preceding and current row) as salary_bucket_avg_2,
        ntile(3) over (partition by manager_id order by seq_no, id) as seq_no_bucket
    from latest
)
-- Grouped metrics
select
    case when salary >= 133000 then 'PREMIUM' else 'STANDARD' end as salary_tier,
    count_if(level = 'L1') as l1_events,
    count(distinct job_title) as distinct_job_titles,
    count(distinct job_title) as distinct_job_titles_2
from scored
group by case when salary >= 133000 then 'PREMIUM' else 'STANDARD' end;
