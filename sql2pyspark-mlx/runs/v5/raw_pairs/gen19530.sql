select departments.dept_name, t.total_salary, t.pct_l3
from departments
left join (select dept_id, sum(salary) as total_salary, round((100 * sum(case when level = 'L3' then 1 else 0 end)) / count(*), 1) as pct_l3
    from employees
    group by dept_id) t on departments.id = t.dept_id
order by t.total_salary desc, dept_name desc
