with employees_by_department as (
    select dept_id, count(case when job_title = 'Engineer' then 1 end) engineer_count, sum(case when job_title = 'Analyst' then salary else 0 end) analyst_salary
    from employees
    group by dept_id
)
select
    d.id,
    d.location,
    d.dept_name,
    ebd.engineer_count,
    ebd.analyst_salary
from departments d
inner join employees_by_department ebd on d.id = ebd.dept_id
