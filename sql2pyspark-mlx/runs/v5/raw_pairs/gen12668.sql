select
    e.full_name,
    e.is_remote,
    d.location,
    d.dept_name
from employees e
left outer join departments d on e.dept_id = d.id
where d.location = 'Bangkok'
order by full_name asc;
