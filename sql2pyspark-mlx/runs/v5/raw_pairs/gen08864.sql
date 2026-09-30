select departments.dept_name, t.engineer_count, t.employee_count
from departments
left join (select dept_id, sum(case when job_title = 'Engineer' then 1 else 0 end) as engineer_count, count(*) as employee_count
    from employees
    group by dept_id) t on departments.id = t.dept_id
where t.employee_count >= 7
order by engineer_count desc;
