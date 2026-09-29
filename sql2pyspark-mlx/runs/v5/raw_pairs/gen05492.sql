with filtered as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        length(email) as email_length
    from employees
    where manager_id is not null
),
joined as (
    select
        a.*,
        d.location,
        d.dept_name,
        d.budget
    from filtered a
    left join departments d
        on a.dept_id = d.id
    where d.dept_name = 'Engineering'
),
joined_employees as (
    select
        a.*,
        p.level as employee_level,
        p.job_title,
        p.full_name as employee_full_name
    from joined a
    left join employees p
        on a.manager_id = p.id
),
cleaned as (
    select
        id,
        email,
        dept_id,
        manager_id,
        salary,
        level,
        dept_name,
        employee_level,
        job_title,
        employee_full_name,
        case when email_length >= 88 then 'High' when email_length >= 29 then 'Medium' else 'Low' end as email_length_band,
        coalesce(job_title, 'N/A') as resolved_job_title
    from joined_employees
    where dept_id in (11, 13, 20, 28)
),
with_windows as (
    select
        *,
        sum(salary) over (partition by job_title order by salary, id rows between 2 preceding and current row) as last3_sum_salary,
        avg(salary) over (partition by job_title order by salary, id rows between 2 preceding and current row) as last3_avg_salary,
        max(salary) over (partition by job_title order by salary, id rows between 2 preceding and current row) as last3_max_salary
    from cleaned
),
matched_keys as (
    select id, dept_id
    from with_windows
    except
    select id, dept_id
    from employees
    where dept_id = 13
),
reconciled as (
    select s.*
    from with_windows s
    inner join matched_keys k on s.id = k.id and s.dept_id = k.dept_id
),
stats as (
    select
        dept_name,
        sort_array(collect_set(level)) as level_list,
        round(sum(last3_avg_salary) / nullif(count(case when dept_name = 'Engineering' then 1 end), 0), 2) as last3_avg_salary_per_engineering,
        sum(case when employee_level = 'L5' then last3_max_salary else 0 end) as l5_last3_max_salary,
        size(collect_set(level)) as num_levels
    from reconciled
    group by dept_name
    having l5_last3_max_salary > 0
)
select
    dept_name,
    level_list,
    last3_avg_salary_per_engineering,
    l5_last3_max_salary,
    num_levels,
    case when dept_name rlike '^(HR|Marketing)$' then 'PRIORITY' when dept_name rlike '^(Engineering|Sales)$' then 'GROUP_A' else 'GROUP_B' end as dept_name_zone
from stats
order by last3_avg_salary_per_engineering, dept_name
limit 10
