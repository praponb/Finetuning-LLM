with scored as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by manager_id order by salary desc) as salary_rank,
        stddev_samp(salary) over w_ordered as level_stddev_samp_salary,
        round(salary / sum(salary) over (partition by level), 4) as salary_share,
        sum(salary) over w_recent as running_salary
    from employees
    window w_ordered as (partition by level), w_recent as (partition by dept_id order by salary, id rows between unbounded preceding and current row)
),
metrics as (
    -- Ranking and gap features
    select
        id,
        email,
        dept_id,
        job_title,
        is_remote,
        level,
        salary_rank,
        level_stddev_samp_salary,
        salary_share,
        running_salary,
        lead(salary_share, 1) over (order by level_stddev_samp_salary, id) as next_salary_share,
        avg(running_salary) over (partition by job_title order by hire_date, id rows between 6 preceding and 1 preceding) as running_salary_avg_6_prior,
        percent_rank() over (partition by level order by level_stddev_samp_salary desc) as level_stddev_samp_salary_percent_rank
    from scored
),
enriched as (
    -- Attach departments attributes
    select
        s.id,
        s.dept_id,
        s.job_title,
        s.level,
        s.level_stddev_samp_salary,
        s.salary_share,
        s.running_salary,
        s.running_salary_avg_6_prior,
        s.level_stddev_samp_salary_percent_rank,
        p.location,
        p.dept_name
    from metrics s
    left join departments p
        on s.dept_id = p.id
),
enriched_employees as (
    -- Running and moving aggregates
    select
        id,
        job_title,
        salary_share,
        running_salary,
        level_stddev_samp_salary_percent_rank,
        location,
        lead(level_stddev_samp_salary_percent_rank, 1) over (order by running_salary, id) as next_level_stddev_samp_salary_percent_rank,
        avg(level_stddev_samp_salary_percent_rank) over (partition by location order by level_stddev_samp_salary, id rows between 2 preceding and 1 preceding) as level_stddev_samp_salary_percent_rank_avg_2_prior,
        count(level_stddev_samp_salary) over (partition by location) as location_count_level_stddev_samp_salary
    from enriched
    where running_salary_avg_6_prior <= 469925
)
-- Multi-level aggregation
select
    job_title,
    round(avg(level_stddev_samp_salary_percent_rank_avg_2_prior), 2) as avg_level_stddev_samp_salary_percent_rank_avg_2_prior,
    sort_array(collect_set(job_title)) as job_title_list,
    count(distinct location) as distinct_locations,
    max(level_stddev_samp_salary_percent_rank) as max_level_stddev_samp_salary_percent_rank,
    max(running_salary) as max_running_salary
from enriched_employees
group by job_title
order by distinct_locations desc
