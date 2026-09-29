select
    dim.dept_name,
    a.level,
    count(*) as num_rows,
    round(sum(a.salary), 2) as total_salary
from employees a
join departments dim on a.dept_id = dim.id
group by dim.dept_name, a.level;
