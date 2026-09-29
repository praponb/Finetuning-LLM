with kept_keys as (
    -- Set difference on keys
    select dept_id
    from employees
    except
    select dept_id
    from employees
    where is_remote
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from employees s
    inner join kept_keys k on s.dept_id = k.dept_id
)
-- Multi-level aggregation
select
    dept_id,
    case when salary >= 151000 then 'PREMIUM' else 'STANDARD' end as salary_tier,
    date_trunc('year', hire_date) as year_start,
    max(salary) filter (where job_title = 'Analyst') as analyst_max_salary,
    round(avg(salary), 2) as avg_salary,
    sum(salary) filter (where level = 'L1') as l1_total_salary
from verified
group by dept_id, case when salary >= 151000 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', hire_date)
order by salary_tier
