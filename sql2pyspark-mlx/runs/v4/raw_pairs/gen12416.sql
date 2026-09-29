with department_summary as (
    select dept_id, sum(case when job_title = 'Analyst' then salary else 0 end) as analyst_salary
    from employees
    where manager_id in (10, 19, 23)
    group by dept_id
)
select d.id, d.dept_name, ds.analyst_salary
from departments d
inner join department_summary ds on d.id = ds.dept_id
order by dept_name desc;
