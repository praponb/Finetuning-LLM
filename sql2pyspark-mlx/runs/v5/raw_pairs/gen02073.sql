with deduped as (
    select *
    from employees
    qualify row_number() over (partition by manager_id, level order by salary, id) = 1
)
select
    s.id,
    s.full_name,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.is_remote,
    s.level,
    case when level rlike '^(L5|L1)$' then 'EXTENDED' when level rlike '^(L2|L3)$' then 'CORE' else 'PRIORITY' end as level_zone,
    (select min(t2.salary) from employees t2 where t2.dept_id = s.dept_id) as min_employee_salary
from deduped s
order by level_zone nulls first;
