with deduped as (
    select *
    from employees
    qualify row_number() over (partition by job_title order by salary, id) = 1
),
enriched as (
    select
        id,
        full_name,
        manager_id,
        hire_date,
        is_remote,
        level,
        first_value(job_title, true) over (partition by level order by hire_date, id rows between unbounded preceding and current row) first_known_job_title,
        stddev_samp(salary) over (partition by level) level_stddev_samp_salary,
        ntile(10) over (order by salary desc, id) salary_bucket
    from deduped
),
metrics as (
    select
        *,
        avg(level_stddev_samp_salary) over (partition by level) level_avg_level_stddev_samp_salary,
        first_value(level_stddev_samp_salary) ignore nulls over (partition by manager_id order by salary_bucket, id rows between unbounded preceding and current row) first_known_level_stddev_samp_salary
    from enriched
),
deduped_employees as (
    select *
    from (
        select *, row_number() over (partition by level order by first_known_level_stddev_samp_salary desc nulls last, id) as dedup_rank
        from metrics
    ) ranked
    where dedup_rank = 1
),
scored as (
    select
        *,
        avg(salary_bucket) over w_running last4_avg_salary_bucket,
        min(salary_bucket) over w_running last4_min_salary_bucket,
        max(salary_bucket) over w_running last4_max_salary_bucket,
        sum(level_avg_level_stddev_samp_salary) over (partition by level) level_sum_level_avg_level_stddev_samp_salary
    from deduped_employees
    window w_running as (partition by level order by salary_bucket, id rows between 3 preceding and current row)
),
enriched_employees as (
    select
        id,
        full_name,
        hire_date,
        level,
        first_known_job_title,
        level_stddev_samp_salary,
        salary_bucket,
        first_known_level_stddev_samp_salary,
        level_sum_level_avg_level_stddev_samp_salary,
        min(level_avg_level_stddev_samp_salary) over (partition by manager_id) manager_id_min_level_avg_level_stddev_samp_salary,
        first_value(level) ignore nulls over (partition by level order by last4_min_salary_bucket, id rows between unbounded preceding and current row) first_known_level,
        round(level_sum_level_avg_level_stddev_samp_salary / sum(level_sum_level_avg_level_stddev_samp_salary) over (partition by manager_id), 4) level_sum_level_avg_level_stddev_samp_salary_share
    from scored
    where full_name like 'b%'
)
select
    id,
    full_name,
    first_known_job_title,
    level_stddev_samp_salary,
    first_known_level_stddev_samp_salary,
    manager_id_min_level_avg_level_stddev_samp_salary,
    first_known_level,
    level_sum_level_avg_level_stddev_samp_salary_share,
    case when full_name rlike '^(Ivy Chen|Alice Wong)$' then 'CORE' when full_name rlike '^(bob smith|  Carol Diaz)$' then 'GROUP_B' else 'OTHER_ZONE' end as full_name_zone
from enriched_employees
order by id desc, level_stddev_samp_salary
