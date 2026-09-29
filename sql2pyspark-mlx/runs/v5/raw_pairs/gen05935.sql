with employees_by_department as (
    select dept_id, sum(case when level = 'L5' then salary else 0 end) as l5_salary, sum(salary) as total_salary
    from employees
    where salary <= 106000
    group by dept_id
)
select
    d.dept_name,
    d.budget,
    ebd.l5_salary,
    ebd.total_salary
from departments d
inner join employees_by_department ebd on d.id = ebd.dept_id;
