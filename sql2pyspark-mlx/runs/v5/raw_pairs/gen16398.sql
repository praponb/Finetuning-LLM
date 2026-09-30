with with_dims as (
    -- Attach departments attributes
    select
        a.*,
        b.location,
        b.dept_name
    from employees a
    left join departments b
        on a.dept_id = b.id
    where b.budget < 3291000
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        avg(salary) over (partition by location, dept_name order by hire_date, id rows between 3 preceding and 1 preceding) as salary_avg_3_prior,
        sum(salary) over (partition by location order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary
    from with_dims
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by manager_id order by salary_avg_3_prior desc, id) as rank_in_group
    from enriched
    qualify rank_in_group = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by location order by salary) as rn
    from deduped
    qualify rn = 1
),
ranked_employees as (
    -- Keep one row per key (latest first)
    select
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        dept_name,
        cumulative_salary
    from ranked
    qualify row_number() over (partition by level order by cumulative_salary desc, id) = 1
),
ranked_employees_2 as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by dept_name, dept_id order by cumulative_salary desc, id) as row_num
    from ranked_employees
    qualify row_num = 1
)
select
    s.id,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.level,
    s.dept_name,
    s.row_num,
    case when dept_name rlike '^(Legal|Engineering)$' then 'PRIORITY' when dept_name rlike '^(Sales|Marketing)$' then 'GROUP_B' else 'EXTENDED' end as dept_name_zone,
    (select max(e2.salary) from employees e2 where e2.dept_id = s.dept_id) as max_employee_salary
from ranked_employees_2 s
order by dept_name desc nulls first, job_title desc
